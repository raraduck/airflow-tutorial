import sys
import argparse
import os
import requests
import json
import jsonlines
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

adm_mapping = {
    "11010":	"종로구",   # 1
    "11020":	"중구",     # 2
    "11030":	"용산구",   # 3
    "11040":	"성동구",   # 4
    "11050":	"광진구",   # 5
    "11060":	"동대문구", # 6
    "11070":	"중랑구",   # 7
    "11080":	"성북구",   # 8
    "11090":	"강북구",   # 9
    "11100":	"도봉구",   #10
    "11110":	"노원구",   #11
    "11120":	"은평구",   #12
    "11130":	"서대문구", #13
    "11140":	"마포구",   #14
    "11150":	"양천구",   #15
    "11160":	"강서구",   #16
    "11170":	"구로구",   #17
    "11180":	"금천구",   #18
    "11190":	"영등포구", #19
    "11200":	"동작구",   #20
    "11210":	"관악구",   #21
    "11220":	"서초구",   #22
    "11230":	"강남구",   #23
    "11240":	"송파구",   #24
    "11250":	"강동구"    #25
}

keys = [
    "year",             # 1 int
    "adm_cd",           # 2 string
    "adm_nm",           # 3 string
    "upper_adm_cd",     # 4 string
    "upper_adm_nm",     # 5 string
    "tot_ppltn",        # 6 int
    "avg_age",          # 7 float
    "ppltn_dnsty",      # 8 float
    "aged_child_idx",   # 9 float
    "oldage_suprt_per", #10 float
    "juv_suprt_per",    #11 float
    "tot_family",       #12 int
    "avg_fmember_cnt",  #13 float
    "tot_house",        #14 int
    "nongga_cnt",       #15 int
    "nongga_ppltn",     #16 int
    "imga_cnt",         #17 int
    "imga_ppltn",       #18 int
    "naesuoga_cnt",     #19 int
    "naesuoga_ppltn",   #20 int
    "haesuoga_cnt",     #21 int
    "haesuoga_ppltn",   #22 int
    "employee_cnt",     #23 int
    "corp_cnt"          #24 int
]

            # "ppltn_dnsty": "42529.3",
            # "oldage_suprt_per": "16.2",
            # "imga_cnt": "N/A",
            # "avg_age": "40.5",
            # "aged_child_idx": "113.5",
            # "naesuoga_cnt": "N/A",
            # "avg_fmember_cnt": "2.5",
            # "haesuoga_cnt": "N/A",
            # "adm_cd": "11040520",
            # "adm_nm": "왕십리2동",
            # "tot_family": "6043",
            # "naesuoga_ppltn": "N/A",
            # "juv_suprt_per": "14.3",
            # "tot_ppltn": "15827",
            # "corp_cnt": "655",
            # "imga_ppltn": "N/A",
            # "employee_cnt": "2229",
            # "nongga_cnt": "N/A",
            # "tot_house": "4037",
            # "nongga_ppltn": "16",
            # "haesuoga_ppltn": "N/A"

def get_access_token(consumer_key, consumer_secret):
    url = "https://sgisapi.kostat.go.kr/OpenAPI3/auth/authentication.json"
    url += f"?consumer_key={consumer_key}" # 전체데이터로 변경
    url += f"&consumer_secret={consumer_secret}"  # 필요한 조회일 등록
    response = requests.get(url)
    res = response.json()
    if res['errCd']==0:
        # print('API 키 획득 성공')
        api_key = res['result']['accessToken']
    else:
        print(f'에러발생: {res["errMsg"]}')
        sys.exit(1)
    return api_key

def get_data_from_api(adm_cd, year):
    if adm_cd == "non":
        url = "https://sgisapi.kostat.go.kr/OpenAPI3/stats/population.json"
        url += '?year=' + str(year)  # 전체데이터로 변경
        url += "&low_search=1"
        url += "&accessToken=" + str(API_KEY)  # 필요한 조회일 등록
    else:
        url = "https://sgisapi.kostat.go.kr/OpenAPI3/stats/population.json"
        url += '?year=' + str(year)  # 전체데이터로 변경
        url += "&adm_cd=" + str(adm_cd)  # 필요한 조회일 등록
        url += "&low_search=1"
        url += "&accessToken=" + str(API_KEY)  # 필요한 조회일 등록

    response = requests.get(url)
    res = response.json()
    if res['errCd']==0:
        return res['result']
    elif res['errCd']==-401:
        print(res['errMsg'])
        return None
    else:
        print(f'에러발생: {res["errMsg"]}')
        sys.exit(1)

def save_file(content, file_name, file_type):
    if file_type == 'json':
        with jsonlines.open(file_name, "w") as f:
            f.write_all(content)
    elif file_type == "json-test":
        with open(file_name, "w", encoding="utf-8") as f:
            json_str = json.dumps(content, ensure_ascii=False, separators=(",", ":"))
            json_str = json_str.replace("[", "[\n").replace("]", "\n]").replace("{", "\t{").replace("},", "},\n")
            f.write(json_str)
    elif file_type == 'csv-test':
        df = pd.DataFrame(content)
        df.to_csv(file_name, index=False, encoding="utf-8-sig")  # utf-8-sig는 엑셀 호환성 ↑
    else:
        print(f"FileType ({file_type}) is not supported")
        sys.exit(1)
    print(f"{file_name}.{file_type} 파일 저장 완료")

def upload_to_s3(file_name, bucket_name, object_name):
    s3_client = boto3.client("s3",
                             aws_access_key_id=AWS_ACCESS_KEY_ID,
                             aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                             region_name=AWS_DEFAULT_REGION,
                             )
    try:
        s3_client.upload_file(file_name, bucket_name, object_name)
    except NoCredentialsError:
        print("AWS 자격 증명을 찾을 수 없습니다.")

def main(args):
    if args.init == 'y':
        args.adm_cd = [str(k) for k in adm_mapping.keys()]
        args.year = [str(year) for year in range(2015, 2024)]
    
    for year in args.year:
        for adm_cd in args.adm_cd:
            # 1. API를 통해서 데이터 가져오기 
            data = get_data_from_api(adm_cd, year)
            # 2. 데이터를 파일로 저장
            file_name = f"{year}_{adm_cd}_result.json"
            save_file(data, file_name, "json")
            # 3. S3에 업로드하기
            bucket_name = "metacode-dwngcp2"
            object_name = f"total_statistics/total_year={year}/total_adm_cd={adm_cd}/result.json"
            upload_to_s3(file_name, bucket_name, object_name)

load_dotenv()
# OpenAPI 키
CONSUMER_KEY = os.getenv("CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("CONSUMER_SECRET")
API_KEY = get_access_token(CONSUMER_KEY, CONSUMER_SECRET)
# AWS 키
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

if __name__ == "__main__":
    sys_argv = sys.argv[1:]
    # print(sys_argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--init', type=str, choices=['y','n'], required=True, help="데이터 수집 초기 여부 (y = Yes, n = No, required)")
    parser.add_argument('-a', '--adm_cd', type=str, nargs="+", default=['11040'], help="행정구역 코드 (5자리), default: 11040 (전국구))")
    parser.add_argument('-y', '--year', type=str, nargs="+", default=['2023'], help="기준연도, default: 2023)")
    args = parser.parse_args(sys_argv)
    # print(args)
    main(args)




