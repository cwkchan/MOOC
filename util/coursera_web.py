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

'''the amount of time to set the implicit wait too'''
SECONDS_TO_WAIT = 15
'''how many times to try and load an individual course page'''
MAX_RETRIES = 5
'''the amount of time to sleep between individual course reloads'''
MAX_RETRIES_WAIT = 10
'''This is the login URL for Coursera which will give back cookies'''
LOGIN_URL = 'https://accounts.coursera.org/signin?mode=signin&post_redirect=%2F'
'''This is the admin page which lists all of the courses'''
ADMIN_URL = 'https://www.coursera.org/admin/'
'''This is the URL to a single course download page'''
SESSION_URL = 'https://www.coursera.org/admin/data/sessions/{}'

def username_and_password_exist(args):
        if args.username is None:
            username = get_properties()['username']
        else:
            username = args.username
        if args.password is None:
            password = get_properties()['password']
        else:
            password = args.password
        if username is None or password is None:
            raise Exception("No username or password found.")
        return (username, password)

def login_coursera_website(browser, username, password):
    try:
        browser.implicitly_wait(SECONDS_TO_WAIT)
        browser.get(LOGIN_URL)
        WebDriverWait(browser, SECONDS_TO_WAIT).until(EC.presence_of_element_located((By.ID, "signin-email")))
        browser.find_element_by_id('signin-email').send_keys(username)
        browser.find_element_by_id('signin-password').send_keys(password)
        browser.find_element_by_class_name("coursera-signin-button").submit()
    except Exception as e:
        print("Signing into Coursera failed. Please check your login information and/or network connection")

def build_coursera_opener(browser):
    try:
        cj = http.cookiejar.CookieJar()
        for item in browser.get_cookies():
            c = http.cookiejar.Cookie(0, item['name'], item['value'], None, None, item['domain'], None, None,
                                  item['path'], None, item['secure'], item['expiry'], None, None, None, None)
            cj.set_cookie(c)
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    except Exception as e:
        print("Creation of Coursera Opener failed")
    return opener