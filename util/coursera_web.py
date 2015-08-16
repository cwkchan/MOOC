# Copyright (C) 2013 The Regents of the University of Michigan
#
# This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see [http://www.gnu.org/licenses/]

from util.config import *

import http.cookiejar
import urllib.request

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = get_logger("coursera_web.py")

SECONDS_TO_WAIT = 15
"""the amount of time to set the implicit wait too"""
MAX_RETRIES = 5
"""how many times to try and load an individual course page"""
MAX_RETRIES_WAIT = 10
"""the amount of time to sleep between individual course reloads"""
LOGIN_URL = 'https://accounts.coursera.org/signin?mode=signin&post_redirect=%2F'
"""This is the login URL for Coursera which will give back cookies"""
ADMIN_URL = 'https://www.coursera.org/admin/'
"""This is the admin page which lists all of the courses"""
SESSION_URL = 'https://www.coursera.org/admin/data/sessions/{}'
"""This is the URL to a single course download page"""


def username_and_password_exist(args):
    """Checks the availability of the username and password according to arguments passed or properties file.
        :return: username and password
    """
    if args.username and args.username.strip():
                username = args.username
    else:
        username = get_properties().get('username', None)

    if args.password and args.password.strip():
                password = args.password
    else:
        password = get_properties().get('password', None)

    if username and username.strip() and password and password.strip():
        return username, password
    else:
        logger.error("Username and/or Password are missing. "
                        "Please update the config files or pass the arguments --username and --password ")
        raise Exception ("Missing username or password")


def login_coursera_website(browser, username, password):
    """Logs into the coursera website using the username password.
    """
    try:
        browser.implicitly_wait(SECONDS_TO_WAIT)
        browser.get(LOGIN_URL)
        WebDriverWait(browser, SECONDS_TO_WAIT).until(EC.presence_of_element_located((By.ID, "signin-email")))
        browser.find_element_by_id('signin-email').send_keys(username)
        browser.find_element_by_id('signin-password').send_keys(password)
        browser.find_element_by_class_name("coursera-signin-button").submit()
    except Exception as e:
        logger.error("Signing into Coursera failed. Please check your login information and/or network connection")

def build_coursera_opener(browser):
    """creates an opener for the for cookies.
        :return: opener
    """
    try:
        cj = http.cookiejar.CookieJar()
        for item in browser.get_cookies():
            c = http.cookiejar.Cookie(0, item['name'], item['value'], None, None, item['domain'], None, None,
                                  item['path'], None, item['secure'], item['expiry'], None, None, None, None)
            cj.set_cookie(c)
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
        return opener
    except Exception as e:
        logger.error("Creation of Coursera Opener failed")
        return None

