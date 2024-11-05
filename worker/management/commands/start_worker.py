import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd
import requests
from django.core.management.base import BaseCommand
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def process_batch_external(batch, cookies, max_retries=3):
    """Process a batch of accounts using cookies."""
    batch_responses = []

    def process_account(user):
        """Process an individual account."""
        url = "https://cts.vnpt.vn/linetest/Test/TestGponByList"
        headers = {"Accept": "application/json, text/plain, */*", "Content-Type": "application/json;charset=UTF-8"}
        payload = {"listInfo": user, "provinceCode": "NAN"}

        for attempt in range(max_retries):
            try:
                response = requests.post(url, json=payload, headers=headers, cookies=cookies)
                if response.status_code == 200:
                    logging.info(f"API request successful for user: {user}")
                    return {"username": user, "response": response.json()}
                logging.warning(f"Attempt {attempt + 1} failed for user {user} - Status code: {response.status_code}")
            except requests.RequestException as e:
                logging.error(f"Network error on attempt {attempt + 1} for user {user}: {e}")
            time.sleep(2)
        logging.error(f"Max retries reached for user: {user}")
        return {"username": user, "response": None}

    with ThreadPoolExecutor() as thread_executor:
        futures = [thread_executor.submit(process_account, user) for user in batch]
        for future in as_completed(futures):
            batch_responses.append(future.result())
    return batch_responses


class Command(BaseCommand):
    help = "Automated login and OTP handling worker."

    def __init__(self):
        super().__init__()
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        self.driver = webdriver.Chrome(options=chrome_options)
        self.telegram_bot_token = "7232952322:AAFmHQi5KepBuP-5uqbO76CVSl7U1q1cSZk"
        self.chat_id = "-4259699197"
        self.cookies_file = "session_cookies.json"
        self.session_cookies = self.load_cookies()
        self.no_response_accounts = []
        self.max_retries = 3

    def load_cookies(self):
        """Load cookies from a JSON file."""
        try:
            with open(self.cookies_file, "r") as file:
                cookies = json.load(file)
                logging.info("Cookies loaded successfully.")
                return cookies
        except (FileNotFoundError, json.JSONDecodeError):
            logging.warning("Cookie file not found or corrupted.")
            return None

    def save_cookies(self, cookies):
        """Save cookies to a JSON file."""
        with open(self.cookies_file, "w") as file:
            json.dump(cookies, file)
        logging.info("Cookies saved successfully.")

    def handle(self, *args, **kwargs):
        """Program entry point."""
        if not self.session_cookies:
            logging.info("No valid cookies found. Logging in...")
            self.auto_login()
        if self.session_cookies:
            self.use_api()

    def auto_login(self):
        """Log into the website and save new cookies."""
        try:
            self.driver.get("https://cts.vnpt.vn/Linetest/Test/TestL2GponPortList")
            self.driver.find_element(By.ID, "username").send_keys("nttung.nan")
            self.driver.find_element(By.ID, "password").send_keys("E@t6mmm2")
            self.driver.find_element(By.XPATH, "//button[text()='ĐĂNG NHẬP']").click()
            self.send_telegram_message("i need otp")

            login_time = datetime.now()
            otp_received = False

            logging.info("Waiting for OTP from Telegram...")
            while not otp_received:
                otp_code = self.get_otp_from_telegram(login_time)
                if otp_code:
                    self.driver.find_element(By.ID, "passOTP").send_keys(otp_code)
                    self.driver.find_element(By.XPATH, "//button[text()='ĐĂNG NHẬP']").click()
                    logging.info("Logged in successfully with OTP.")
                    otp_received = True
                    self.session_cookies = {cookie['name']: cookie['value'] for cookie in self.driver.get_cookies()}
                    self.save_cookies(self.session_cookies)
                elif datetime.now() - login_time > timedelta(minutes=10):
                    logging.error("OTP not received within 10 minutes. Exiting.")
                    self.driver.quit()
                    return
                else:
                    time.sleep(5)
        finally:
            self.driver.quit()

    def send_telegram_message(self, message):
        """Send a message via Telegram."""
        url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": message}
        try:
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                logging.info("Message sent to Telegram.")
            else:
                logging.error("Failed to send message to Telegram.")
        except requests.RequestException as e:
            logging.error(f"Telegram message error: {e}")

    def get_otp_from_telegram(self, login_time):
        """Retrieve OTP from Telegram after login."""
        url = f"https://api.telegram.org/bot{self.telegram_bot_token}/getUpdates"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                messages = response.json().get("result", [])
                for message in reversed(messages):
                    if "text" in message["message"] and message["message"]["chat"]["id"] == int(self.chat_id):
                        message_time = datetime.fromtimestamp(message["message"]["date"])
                        if message_time > login_time:
                            otp_code = message["message"]["text"]
                            if otp_code.isdigit():
                                return otp_code
            else:
                logging.warning("Failed to retrieve OTP from Telegram.")
        except requests.RequestException as e:
            logging.error(f"Error retrieving OTP from Telegram: {e}")
        return None

    def use_api(self):
        """Fetch and process account data in batches."""
        start_time = time.time()
        cookies = self.session_cookies

        try:
            user_data = pd.read_excel("TB.xlsx")
        except FileNotFoundError:
            logging.error("File TB.xlsx not found.")
            return

        user_list = user_data["Tài khoản FiberVNN"].tolist()
        all_responses = []

        with ProcessPoolExecutor(max_workers=5) as process_executor:
            batch_size = 10
            batches = [user_list[i:i + batch_size] for i in range(0, len(user_list), batch_size)]
            futures = [process_executor.submit(process_batch_external, batch, cookies) for batch in batches]

            for future in as_completed(futures):
                try:
                    batch_response = future.result()
                    for response in batch_response:
                        if response["response"] is None:
                            self.no_response_accounts.append(response["username"])
                        else:
                            all_responses.append(response)
                except Exception as e:
                    logging.error(f"Batch processing error: {e}")

        if self.no_response_accounts:
            logging.info("Retrying accounts with no response.")
            retry_responses = self.retry_no_response_accounts(cookies)
            all_responses.extend(retry_responses)

        responses_data = pd.json_normalize(
            [{"username": r["username"], **resp} for r in all_responses if r["response"] for resp in r["response"]]
        )
        responses_data.to_excel("user_response_results.xlsx", index=False, engine="openpyxl")
        logging.info("Responses saved to 'user_response_results.xlsx'.")

        total_time = time.time() - start_time
        logging.info(f"Total execution time: {total_time:.2f} seconds")

    def retry_no_response_accounts(self, cookies):
        """Retry processing accounts that received no response initially."""
        retry_responses = []
        with ThreadPoolExecutor() as thread_executor:
            futures = [thread_executor.submit(self.process_account, user, cookies) for user in
                       self.no_response_accounts]
            for future in as_completed(futures):
                try:
                    response = future.result()
                    if response:
                        retry_responses.append(response)
                except Exception as e:
                    logging.error(f"Retry error for account: {e}")
        return retry_responses
