"""
데이터 안심구역에 올린 파일들을 일괄적으로 지우는 프로세스 (Selenium + Requests Hybrid)
Selenium으로 암호화 로그인을 처리하고,
로그인된 세션 정보를 Requests로 가져와 빠른 API 요청으로 삭제를 수행
apId 기준으로 삭제처리
"""
import requests
import os
import time
import random
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- 1. 초기 설정 ---
load_dotenv()

BASE_URL = "https://dsz.kdata.or.kr"
LOGIN_PAGE_URL = f"{BASE_URL}/login.do"
DELETE_URL = f"{BASE_URL}/member/apply/mine/takeout_cancel.do"
MY_APPLY_LIST_URL = f'{BASE_URL}/member/apply/mine/takeout_total_list.do'

APPLICATION_ID = 'apply_9d8668c7d7fb4b909c316e6e8b71a5c0'

USER_ID = os.getenv("DSZ_ID")
USER_PW = os.getenv("DSZ_PASSWORD")

# 제거할 apId 입력
DELETE_ITEMS = [101774, 101788, 102181, 102188, 102191, 102198, 102206, 102208, 102210, 102212, 102214, 102216, 102218, 102220, 102222, 102224, 102226, 102231, 102233, 102235, 102241, 102243, 102245, 102247, 102249, 102251, 102253, 102255, 102257, 102259, 102263, 102267, 102269, 102271, 102273, 102588, 102590, 102592, 102594, 102596, 102598, 102600, 102602, 102604, 102606, 102608, 102610, 102612, 102614, 102616, 102618, 102620, 102622, 102624, 102626, 102628, 102630, 102632, 102634, 102638, 102642, 102644, 102646, 102648, 102651, 102653, 102655, 102657, 102659, 102661, 102663, 102665, 102667, 102669, 102671, 102673, 102675, 102677, 102679, 102681, 102683, 102685, 102687, 102689, 102691, 102695, 102699, 102701, 102703, 102705, 102707, 102709, 102711, 102713, 102715, 102717, 102719, 102721, 102723, 102725, 102727, 102729, 102731, 102733, 102735, 102737, 102739, 102741, 102743, 102745, 102747, 102751, 102755, 102757, 102759, 102761, 102763, 102765, 102767, 102769, 102771, 102773, 102775, 102777, 102779, 102781, 102783, 102785, 102787, 102789,
 102791, 102793, 102795, 102797, 102799, 102801, 102803, 102805, 102807, 102809, 102811, 102813, 102815, 102817, 102821, 102825, 102827, 102829, 102831, 102833, 102835, 102837, 102839, 102841, 102843, 102845, 102847, 102849, 102851, 102853, 102855, 102857, 102859, 102861, 102863, 102865, 102867, 102869, 102871, 102873, 102877, 102881, 102883, 102885, 102887, 102889, 102891, 102893, 102895, 102897, 102899, 102901, 102903, 102905, 102907, 102909, 102911, 102913, 102915, 102917, 102919, 102921, 102923, 102925, 102927, 102929, 102933, 102937, 102939, 102941, 102943, 102945, 102947, 102949, 102951, 102953, 102955, 102957, 102959, 102961, 102963, 102965, 102967, 102969, 102971, 102973, 102975, 102977, 102979, 102981, 102983, 102985, 102987, 102989, 102991, 102993, 102995, 102997, 102999, 103003, 103007, 103009, 103011, 103013, 103015, 103017, 103019, 103021, 103023, 103025, 103027, 103029, 103031, 103033, 103035, 103037, 103039, 103041, 103043, 103045, 103047, 103049, 103051, 103053, 103055, 103059, 103063, 103065, 103067, 103069, 103071, 103073, 103075, 103077, 103079, 103081, 103083, 103085, 103087, 103089, 103091, 103093, 103095, 103097, 103099, 103101, 103103, 103105, 103107, 103109, 103111, 103113, 103115, 103117, 103119, 103121, 103123, 103125, 103129, 103133, 103135, 103137, 103139, 103141, 103143, 103145, 103147, 103149, 103151, 103153, 103155, 103157, 103159, 103161, 103163, 103165, 103167, 103169, 103171, 103173, 103175, 103177, 103179, 103181, 103185, 103189, 103191, 103193, 103195, 103197, 103199, 103201, 103203, 103205, 103207, 103209, 103211, 103213, 103215, 103217, 103219, 103221, 103223, 103225, 103227, 103229, 103231, 103233, 103235, 103237, 103241, 103245, 103247, 103249, 103251, 103253, 103255, 103257, 103259, 103261, 103263, 103265, 103267, 103269, 103271, 103273, 103275, 103277, 103279, 103281, 103283, 103285, 103287, 103289, 103291, 103293, 103295, 103297, 103299, 103301, 103303, 103305, 103307, 103311, 103315, 103317, 103319, 103321, 103323, 103325, 103327, 103329, 103331, 103333, 103335, 103337, 103339, 103341, 103343, 103345, 103347, 103349, 103351, 103353, 103355, 103357, 103359, 103361, 103363, 103367, 103371, 103373, 103375, 103377, 103379, 103381, 103383, 103385, 103387, 103389, 103391, 103393, 103395, 103397, 103399, 103401, 103403, 103405, 103407, 103409, 103411, 103413, 103415, 103417, 103419, 103423, 103427, 103429, 103431, 103433, 103435, 103437, 103439, 103441, 103443, 103445, 103447, 103449, 103451, 103453, 103455, 103457, 103459, 103461, 103463, 103465, 103467, 103469, 103471, 103473, 103475, 103479, 103483, 103485, 103487, 103489, 103491, 103493, 103495, 103497, 103499, 103501, 103503, 103505, 103507, 103509, 103511, 103513, 103515, 103517, 103519, 103521, 103523, 103525, 103527, 103529, 103531, 103533, 103535, 103537, 103539, 103541, 103543, 103545, 103549, 103553, 103555, 103557, 103559, 103561, 103563, 103565, 103567, 103569, 103571, 103573, 103575, 103577, 103579, 103581, 103583, 103585, 103587, 103589, 103591, 103593, 103595, 103597, 103599, 103601, 103605, 103609, 103611, 103613, 103615, 103617, 103619, 103621, 103623, 103625, 103627, 103629, 103631, 103633, 103635, 103637, 103639, 103641, 103643, 103645, 103647, 103649, 103651, 103653, 103655, 103657, 103659, 103661, 103663, 103665, 103667, 103669, 103671, 103675, 103679, 103681, 103690
]

if not USER_ID or not USER_PW:
    print("오류: .env 파일에 DSZ_ID와 DSZ_PASSWORD를 설정해주세요.")
    exit()

# --- 2. 자동화 로직 클래스 (Selenium Hybrid) ---
class DataSafetyZoneAutomator:
    def __init__(self, user_id, user_pw):
        self.user_id = user_id
        self.user_pw = user_pw
        self.driver = None
        # requests 세션 객체를 미리 준비합니다.
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Referer": f"{BASE_URL}/"  # 요청 시 Referer 헤더를 추가하면 도움이 될 수 있습니다.
        })
        print("자동화 준비 완료.")

    def login(self):
        """Selenium을 이용해 브라우저로 로그인하고 세션을 requests로 가져옵니다."""
        print("Selenium 브라우저를 사용하여 로그인을 시작합니다...")
        try:
            # ChromeDriver를 자동으로 설치하고 실행합니다.
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service)
            self.driver.get(LOGIN_PAGE_URL)


            wait = WebDriverWait(self.driver, 10)
            id_input = wait.until(EC.presence_of_element_located((By.ID, "userId")))
            pw_input = self.driver.find_element(By.ID, "userPw")

            id_input.send_keys(self.user_id)
            pw_input.send_keys(self.user_pw)


            print("로그인 버튼 클릭...")
            self.driver.find_element(By.ID, "btnLogin").click()  # 예시: class 이름이 'btn_login'인 버튼

            # 비밀번호변경안내: 다음에 변경 버튼 클릭
            time.sleep(3)
            self.driver.find_element(By.ID, "btnCancel").click()
            # 로그인이 성공하고 페이지가 이동할 때까지 최대 15초 대기
            print("Selenium 로그인 성공!")

            # --- 핵심: Selenium의 쿠키를 requests 세션으로 복사 ---
            print("로그인된 세션 정보를 requests로 이전합니다...")
            cookies = self.driver.get_cookies()
            for cookie in cookies:
                self.session.cookies.set(cookie['name'], cookie['value'])

            return True

        except Exception as e:
            print(f"Selenium 로그인 중 오류 발생: {e}")
            return False
        finally:
            # 로그인 과정이 끝나면 브라우저는 더 이상 필요 없으므로 종료합니다.
            if self.driver:
                self.driver.quit()
                print("Selenium 브라우저를 종료했습니다.")

    def delete_item(self, item_id):
        """로그인된 requests 세션을 사용하여 아이템을 삭제합니다."""

        print(f"아이템 '{item_id}' 삭제 시도 중 (requests 사용)")
        delete_data = {
            'apId': item_id,
            'applicationId': APPLICATION_ID
        }
        try:
            # 이제부터 requests로 API 요청을 보냅니다.
            response = self.session.post(DELETE_URL, data=delete_data)
            response.raise_for_status()

            # 응답이 JSON 형태일 수 있으므로 .json()으로 파싱 시도
            try:
                result = response.json()
                if result.get('result') == 'success' or result.get('code') == '200':
                    print(f"-> 아이템 '{item_id}' 삭제 완료")
                    return True
            except requests.exceptions.JSONDecodeError:
                # JSON이 아닌 일반 텍스트 응답일 경우
                if "삭제" in response.text or "성공" in response.text:
                    print(f"-> 아이템 '{item_id}' 삭제 완료 (텍스트 기반 확인)")
                    return True

            print(f"-> 아이템 '{item_id}' 삭제 실패")
            print(response.text[:500])
            return False

        except requests.exceptions.RequestException as e:
            print(f"삭제 요청 중 오류 발생: {e}")
            return False

# --- 3. 스크립트 실행 ---
if __name__ == "__main__":
    automator = DataSafetyZoneAutomator(USER_ID, USER_PW)

    # 1. 로그인 수행 (Selenium)
    if automator.login():
        # 2. 로그인 성공 시, 삭제 작업 진행 (requests)
        print(f'\n로그인 성공! {len(DELETE_ITEMS)}개의 아이템 삭제를 시작합니다.')

        success_count = 0
        fail_count = 0

        for item in DELETE_ITEMS:
            if automator.delete_item(item):
                success_count += 1
            else:
                fail_count += 1
            time.sleep(random.uniform(1, 4))  # 서버에 부담을 주지 않기 위해 각 요청 사이에 1초 대기

        print(f"\n--- 작업 완료 ---")
        print(f"성공: {success_count}건, 실패: {fail_count}건")