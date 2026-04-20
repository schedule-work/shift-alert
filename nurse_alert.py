import gspread
import requests
import time
import os
import json
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# [1] 구글 시트 연결 (시트 ID 방식)
def connect_sheet():
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if creds_json:
        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict)
    else:
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json')
    
    client = gspread.authorize(creds)
    # 알려주신 시트 ID 사용
    return client.open_by_key("1tNobsqOTDzIKwAcF0VfUanRTSZCArqIF63n5AxKfDbc").worksheet("대체간호사 근무표")

# [2] ntfy 발송 함수
def send_ntfy(topic, message, title):
    # 특수문자 제거 후 대문자 고정
    topic = "".join(filter(str.isalnum, topic)).upper()
    url = f"https://ntfy.sh/{topic}"
    
    headers = {
        "Title": title.encode('utf-8'),
        "Priority": "high",
        "Tags": "hospital,bell"
    }
    
    try:
        response = requests.post(url, data=message.encode('utf-8'), headers=headers)
        return response.status_code == 200
    except Exception as e:
        print(f"❌ 발송 실패 ({topic}): {e}")
        return False

# [3] 내일 날짜의 열 인덱스 자동 계산 (F열=6, 월 변경 시 +2)
def get_target_column(target_date):
    base_date = datetime(2026, 3, 1) # 기준일: 2026년 3월 1일
    base_col = 6  # F열
    
    current = base_date
    col_idx = base_col
    
    while current < target_date:
        prev_month = current.month
        current += timedelta(days=1)
        # 월이 바뀌면 공백열(+1) + 1일(+1) = 2칸 이동
        if current.month != prev_month:
            col_idx += 2 
        else:
            col_idx += 1
    return col_idx

# [4] 메인 로직
def main():
    try:
        print("1. 구글 시트 연결 시도 중...")
        sheet = connect_sheet()
        print("✅ 시트 연결 성공!")

        # 내일 날짜 계산 (한국 시간 기준)
        tomorrow = datetime.now() + timedelta(days=1)
        tomorrow = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
        
        col_idx = get_target_column(tomorrow)
        print(f"2. {tomorrow.date()} 데이터를 {col_idx}번째 열에서 읽습니다.")

        all_data = sheet.get_all_values()
        
        nurse_map = {}
        skip_list = ["ba", "ca", "pa", "ha", "sa", "off", "-", "", "/", " "]

        # 데이터 분석
        for row in all_data[1:]: # 헤더 제외
            if len(row) < col_idx: continue
            
            sid = str(row[1]).strip()    # B열: 사번
            name = str(row[2]).strip()   # C열: 성함
            kind = str(row[4]).strip()   # E열: 구분
            duty_val = str(row[col_idx-1]).strip() # 내일 근무

            if not sid or sid == "사번" or "프리셉터" in kind:
                continue

            if sid not in nurse_map:
                nurse_map[sid] = {"name": name, "duty": "근무", "alt": "", "sup": ""}

            val_lower = duty_val.lower()
            if val_lower not in skip_list:
                if kind == "":
                    nurse_map[sid]["duty"] = duty_val
                elif "대체" in kind:
                    nurse_map[sid]["alt"] = duty_val.upper()
                elif "지원" in kind:
                    nurse_map[sid]["sup"] = duty_val.upper()

        date_str = tomorrow.strftime("%m/%d") + "(" + ["월","화","수","목","금","토","일"][tomorrow.weekday()] + ")"
        
        # 발송 시작
        for sid, n in nurse_map.items():
            # 💡 [요청반영] kugr_dns_사번 형식 (p_ 제거)
            p_topic = f"kugr_dns_{sid}"
            
            for mode in ["alt", "sup"]:
                if n[mode]:
                    type_kr = "대체" if mode == "alt" else "지원"
                    ward_topic = f"kugr_dns_{n[mode]}"
                    
                    msg = f"꿈마스터 {n['name']} 선생님, {date_str} [{n['duty']}] {n[mode]} {type_kr} 근무입니다."
                    title_str = f"[교대제 {type_kr}근무 알림]"
                    
                    # 1. 병동 채널 발송
                    send_ntfy(ward_topic, msg, title_str)
                    # 2. 개인 채널 발송
                    send_ntfy(p_topic, msg, title_str)
                    
                    print(f"✅ {n['name']} 선생님 ({n[mode]}) 발송 완료")
                    time.sleep(4)

    except Exception as e:
        print(f"❌ 실행 중 오류 발생: {e}")
        raise e

if __name__ == "__main__":
    main()
