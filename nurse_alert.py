import gspread
import requests
import time
import os
import json
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# [1] 구글 시트 연결
def connect_sheet():
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if creds_json:
        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict)
    else:
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json')
    
    client = gspread.authorize(creds)
    return client.open_by_key("1tNobsqOTDzIKwAcF0VfUanRTSZCArqIF63n5AxKfDbc").worksheet("대체간호사 근무표")

# [2] ntfy 발송 함수 (아이콘 제거)
def send_ntfy(topic, message, title):
    url = f"https://ntfy.sh/{topic.strip()}"
    headers = {
        "Title": title.encode('utf-8'),
        "Priority": "high"
        # Tags를 넣지 않아 아이콘을 제거했습니다.
    }
    try:
        response = requests.post(url, data=message.encode('utf-8'), headers=headers)
        return response.status_code == 200
    except Exception as e:
        print(f"❌ 발송 실패 ({topic}): {e}")
        return False

# [3] 내일 날짜 열 인덱스 계산
def get_target_column(target_date):
    base_date = datetime(2026, 3, 1)
    base_col = 6
    current = base_date
    col_idx = base_col
    while current < target_date:
        prev_month = current.month
        current += timedelta(days=1)
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

        tomorrow = datetime.now() + timedelta(days=1)
        tomorrow = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
        
        col_idx = get_target_column(tomorrow)
        print(f"2. {tomorrow.date()} 데이터를 {col_idx}번째 열에서 읽습니다.")

        all_data = sheet.get_all_values()
        nurse_map = {}
        summary_list = [] # 💡 총괄 리포트를 담을 바구니 추가
        skip_list = ["ba", "ca", "pa", "ha", "sa", "off", "-", "", "/", " "]

        for row in all_data[1:]:
            if len(row) < col_idx: continue
            
            sid = str(row[1]).strip()
            name = str(row[2]).strip()
            kind = str(row[4]).strip()
            duty_val = str(row[col_idx-1]).strip()

            if not sid or sid == "사번" or "프리셉터" in kind: continue

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
        
        for sid, n in nurse_map.items():
            p_topic = f"kugr_dns_{sid.upper()}"
            
            for mode in ["alt", "sup"]:
                if n[mode]:
                    type_kr = "대체" if mode == "alt" else "지원"
                    ward_topic = f"kugr_dns_{n[mode]}"
                    
                    msg = f"{n['name']} 선생님, {date_str} [{n['duty']}] {n[mode]} {type_kr} 근무입니다."
                    title_str = f"[교대제 {type_kr}근무 알림]"
                    
                    # 개별 발송
                    send_ntfy(ward_topic, msg, title_str)
                    send_ntfy(p_topic, msg, title_str)
                    
                    # 💡 총괄 리포트 내용 추가
                    summary_list.append(f"- {n['name']} ({n[mode]} {type_kr})")
                    
                    print(f"✅ {n['name']} -> {ward_topic} & {p_topic} 발송 완료")
                    time.sleep(4)

        # 💡 [총괄 리포트 발송] 반복문이 다 끝난 후 마지막에 실행
        if summary_list:
            summary_msg = f"{date_str} 교대제 근무 현황\n\n" + "\n".join(summary_list)
            send_ntfy("kugr_dns", summary_msg, "[교대제 근무 총괄 현황]")
            print(f"📢 총괄 현황 발송 완료 (kugr_dns)")

    except Exception as e:
        print(f"❌ 실행 중 오류 발생: {e}")
        raise e

if __name__ == "__main__":
    main()
