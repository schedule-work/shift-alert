import gspread
import requests
import time
import os
import json
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

def connect_sheet():
    # 깃허브 실행 시 환경변수에서, 로컬 테스트 시 파일에서 인증 정보 로드
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if creds_json:
        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict)
    else:
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json')
    
    client = gspread.authorize(creds)
    # 스프레드시트 이름 정확히 입력
    return client.open("대체간호사 근무표").worksheet("대체간호사 근무표")

def get_target_column(target_date):
    """F열(6)이 2026-03-01 기준일 때, 월 변경 공백(+2) 계산"""
    base_date = datetime(2026, 3, 1)
    base_col = 6
    current = base_date
    col_idx = base_col
    
    while current < target_date:
        prev_month = current.month
        current += timedelta(days=1)
        # 월이 바뀌면 공백(1) + 1일(1) = 2칸 이동
        col_idx += 2 if current.month != prev_month else 1
    return col_idx

def send_ntfy(topic, message, title):
    """ntfy 푸시 발송 (대문자 고정 및 인코딩)"""
    topic = "".join(filter(str.isalnum, topic)).upper()
    headers = {
        "Title": title.encode('utf-8'),
        "Priority": "high",
        "Tags": "hospital,bell"
    }
    try:
        res = requests.post(f"https://ntfy.sh/{topic}", data=message.encode('utf-8'), headers=headers)
        return res.status_code == 200
    except:
        return False

def main():
    sheet = connect_sheet()
    tomorrow = datetime.now() + timedelta(days=1)
    tomorrow = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
    
    col_idx = get_target_column(tomorrow)
    # 전체 행 데이터 한 번에 가져오기 (가장 빠름)
    all_data = sheet.get_all_values()
    
    nurse_map = {}
    skip_list = ["ba", "ca", "pa", "ha", "sa", "off", "-", "", "/", " "]

    for row in all_data[1:]: # 헤더 제외
        if len(row) < col_idx: continue
        
        sid, name, kind = row[1].strip(), row[2].strip(), row[4].strip()
        duty = row[col_idx-1].strip() # 리스트 인덱스는 0부터라 -1

        if not sid or sid == "사번" or "프리셉터" in kind: continue
        if sid not in nurse_map:
            nurse_map[sid] = {"name": name, "duty": "근무", "alt": "", "sup": ""}

        if duty.lower() not in skip_list:
            if kind == "": nurse_map[sid]["duty"] = duty
            elif "대체" in kind: nurse_map[sid]["alt"] = duty.upper()
            elif "지원" in kind: nurse_map[sid]["sup"] = duty.upper()

    date_str = tomorrow.strftime("%m/%d") + "(" + ["월","화","수","목","금","토","일"][tomorrow.weekday()] + ")"
    
    for sid, n in nurse_map.items():
        # 대체/지원 근무가 있을 때만 발송
        for mode in ["alt", "sup"]:
            if n[mode]:
                type_kr = "대체" if mode == "alt" else "지원"
                msg = f"꿈마스터 {n['name']} 선생님, {date_str} [{n['duty']}] {n[mode]} {type_kr} 근무입니다."
                
                # 병동 및 개인 채널 전송
                send_ntfy(f"kugr_dns_{n[mode]}", msg, f"교대제 {type_kr}근무 알림")
                send_ntfy(f"kugr_dns_p_{sid}", msg, f"교대제 {type_kr}근무 알림")
                
                print(f"✅ 발송 완료: {n['name']} 선생님 ({n[mode]})")
                time.sleep(4) # 차단 방지용 4초 대기

if __name__ == "__main__":
    main()
