#    Copyright (C) 2013 The Regents of the University of Michigan
#
#    This program is free software: you can redistribute it and/or modify
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
#    along with this program.  If not, see [http://www.gnu.org/licenses/].

import enchant
from enchant.checker import SpellChecker
from enchant.tokenize import get_tokenizer, HTMLChunker, EmailFilter, URLFilter 
from readability_score.calculators.fleschkincaid import FleschKincaid
from readability_score.common import getTextScores
import re
from StringIO import StringIO

def generate_post_statistics(text,spell_checking_locale="en_US",hyphenation_dictionary='/usr/share/myspell/dicts/hyph_en_US.dic'):
	"""Creates a number of statistics of a forum post including:
	- a list of emails
	- a list of urls
	- number of misspelt works
	- Flesch-Kincaid readability score
	- a list of the spell checked text
	These results are only meaningful for (US) english text.  Two dictionaries are
	used, one for spell checking and one for hyphenation.  The first is provided
	as a locale (e.g. "en_US") that maps to a dictionary installed in enchant,
	the second as a filepath to the hyphenation dictionary that should be used
	for syllabul detection (e.g. /usr/share/myspell/dicts/hyph_en_US.dic).
	"""
	#spell checking first to get cleaner data for f-k
	#create dict and tokenizer at this level to save on reallocation
	dic = enchant.Dict(spell_checking_locale)
	tknzr = get_tokenizer(spell_checking_locale,(HTMLChunker,),(EmailFilter,URLFilter))
	
	sentances=__sentances_from_post(text)
	clean_sentances=[]
	misspellings=0
	for sentance in sentances:
		sentance_stats=__spell_check_sentance(sentance,dic,tknzr)
		corrected_string=sentance_stats["corrected_string"]
		if len(corrected_string) >0:
			clean_sentances.append(corrected_string)
		misspellings+=sentance_stats["misspelt_words"]
	clean_text=". ".join(clean_sentances)

	#run f-k, from http://en.wikipedia.org/wiki/Flesch%E2%80%93Kincaid_readability_tests
	scores=__readability_score_from_post(clean_text,locale=hyphenation_dictionary)
	scores["misspellings"]=misspellings
	scores["correct_post_text"]=clean_text

	#pull out emails and urls
	urls,emails=__urls_and_emails_from_post(text)
	scores["emails"]=" ".join(emails)
	scores["urls"]=" ".join(urls)
	return scores

def __readability_score_from_post(text,locale):
	"""Calculates the readbility score using Flesch-Kincaid.
	"""
	fk = FleschKincaid(StringIO(text.encode("utf_8")).read(), locale)
	fk.scores["min_age"]=fk.min_age
	fk.scores["us_grade"]=fk.us_grade
	try:
		fk.scores["fk_readability"]=206.835-1.015*(fk.scores["word_count"]/fk.scores["sent_count"])-84.6*(fk.scores["syll_count"]/fk.scores["word_count"])
	except:
		fk.scores["fk_readability"]=None
	return fk.scores

def __spell_check_sentance(text,dic,tknzr):
	"""Spell checks the given text, return various characteristics about it
	"""
	corrected_words=[]
	score=0
	for word in tknzr(text):
		if (dic.check(word[0])):
			corrected_words.append(word[0])
		else:
			score=score+1
	corrected_string=" ".join(corrected_words)
	return {"corrected_string": corrected_string.strip(), "misspelt_words": score}

def __urls_and_emails_from_post(text):
	"""Returns a list of URLS or emails that might be in the posting text
	"""
	#get URLS in message: http://stackoverflow.com/questions/6883049/regex-to-find-urls-in-string-in-python
	urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
	#get email addresses in message: http://pythonhosted.org/pyenchant/api/enchant.tokenize.html
	emails = re.findall(r'^.+@[^\.].*\.[a-z]{2,}$', text)
	return (urls,emails)
		
def __sentances_from_post(text):
	"""Attempts to return a list of sentances from text.  Splits on .?! as 
	well as newline characters.
	"""
	text_sentances=[]
	#a newline character should be treated as an implied sentance end
	newline_paragraphs=text.split("\n")
	#sentance end punctuation followed by a space can be considered a sentance end too
	for paragraph in newline_paragraphs:
		sentances = re.findall(r'[A-Za-z][^.!?]*[.!?]', paragraph)
		if len(sentances)==0:
			#treat whole paragraph as a setances
			if len(paragraph)!=0:
				text_sentances.append(paragraph)
		for sentance in sentances:
			text_sentances.append(sentance)
	return text_sentances