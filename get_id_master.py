from bs4 import BeautifulSoup as soup
import requests
import pandas as pd

list_umkm_id = []
log = []
def contain(containers, list_):
	for container in containers:
		c = container.text
		try: c = int(c)
		except: c = c
		if type(c) is int and len(str(c)) == 15:
			list_.append(str(c))
def string_number(text):
	text = str(text)
	if len(text) == 1:
		text = '0' + text
	return text

df = pd.read_csv('i.csv')

save_every = 10
save_every_bak = save_every

url = 'http://umkm.depkop.go.id/'
session = requests.session()

r = session.get(url)

page_html = r.content
page_soup = soup(page_html, "html.parser")

containers_input = page_soup.findAll("input")
VIEWSTATE = containers_input[0]['value']
# VIEWSTATEGENERATOR = containers_input[1]['value']
VIEWSTATEENCRYPTED = containers_input[1]['value']
EVENTVALIDATION = containers_input[2]['value']
#EVENTTARGET = 'ctl00$MainContent$GridView1'
EVENTTARGET = 'ctl00$MainContent$DropDownList1'
containers_td = page_soup.findAll("td")


#ambil di awal
#contain(containers_td,list_umkm_id)
print(r)
no_page = 1
EVENTARGUMENT = 'Page$1'
no_prov = string_number(35)
no_kab = string_number(1)

data = {'__EVENTTARGET': EVENTTARGET,'__EVENTARGUMENT': EVENTARGUMENT,'__LASTFOCUS': '','__VIEWSTATE': VIEWSTATE,'__VIEWSTATEENCRYPTED':VIEWSTATEENCRYPTED,'__EVENTVALIDATION':EVENTVALIDATION,'ctl00$MainContent$DropDownList1': no_prov,'ctl00$MainContent$DropDownList2': no_kab,'ctl00$MainContent$TextBox1': ''}
r = session.post(url, data=data)
print(r)
status = '-'

if r.status_code == requests.codes.ok:
    page_html = r.content
    page_soup = soup(page_html, "html.parser")
    containers_td = page_soup.findAll("td")
    contain(containers_td,list_umkm_id)

    EVENTTARGET = 'ctl00$MainContent$DropDownList1'
    containers_input = page_soup.findAll("input")
    VIEWSTATE = containers_input[0]['value']
    # VIEWSTATEGENERATOR = containers_input[1]['value']
    VIEWSTATEENCRYPTED = containers_input[1]['value']
    EVENTVALIDATION = containers_input[2]['value']

    status = 'OK'
    log.append({
        'no_prov': no_prov,
        'no_kab': no_kab,
        'no_page': str(no_page),
        'status': status
        })
else:
    EVENTTARGET = 'ctl00$MainContent$DropDownList1'

    status = 'Error'
    print(no_prov, no_kab, str(no_page), r, '.Jumlah: ' + str(len(list_umkm_id)))
    log.append({
        'no_prov': no_prov,
        'no_kab': no_kab,
        'no_page': str(no_page),
        'status': status
        })
    no_page = 0

no_prov = string_number(35)
no_kab = string_number(24)


no_page = 0
for index, row in df.iterrows():
	no_prov = string_number(35)
	no_kab = string_number(24)
	while True:
		no_page += 1
		if no_page == 0:
			EVENTARGUMENT = ''
		else:
			EVENTARGUMENT = 'Page$' + str(no_page)


		data = {'__EVENTTARGET': EVENTTARGET,'__EVENTARGUMENT': EVENTARGUMENT,'__LASTFOCUS': '','__VIEWSTATE': VIEWSTATE,'__VIEWSTATEENCRYPTED':VIEWSTATEENCRYPTED,'__EVENTVALIDATION':EVENTVALIDATION,'ctl00$MainContent$DropDownList1': no_prov,'ctl00$MainContent$DropDownList2': no_kab,'ctl00$MainContent$TextBox1': ''}
		r = session.post(url, data=data)

		status = '-'

		if r.status_code == requests.codes.ok:
			page_html = r.content
			page_soup = soup(page_html, "html.parser")
			containers_td = page_soup.findAll("td")
			contain(containers_td,list_umkm_id)

			EVENTTARGET = 'ctl00$MainContent$GridView1'
			containers_input = page_soup.findAll("input")
			VIEWSTATE = containers_input[0]['value']
			# VIEWSTATEGENERATOR = containers_input[1]['value']
			VIEWSTATEENCRYPTED = containers_input[1]['value']
			EVENTVALIDATION = containers_input[2]['value']

			status = 'OK'
			log.append({
				'no_prov': no_prov,
				'no_kab': no_kab,
				'no_page': str(no_page),
				'status': status
				})
		else:
			EVENTTARGET = 'ctl00$MainContent$DropDownList1'
			
			status = 'Error'
			print(no_prov, no_kab, str(no_page), r, '.Jumlah: ' + str(len(list_umkm_id)))
			log.append({
				'no_prov': no_prov,
				'no_kab': no_kab,
				'no_page': str(no_page),
				'status': status
				})
			no_page = 0
			break

		print( no_prov, no_kab, str(no_page) , r, '.Jumlah: ' + str(len(list_umkm_id)) , str(save_every))
		save_every -= 1
		if save_every  < 0:
			df_umkm = pd.DataFrame.from_dict(list_umkm_id)
			df_umkm.to_csv('umkm.csv')
			df_log = pd.DataFrame.from_dict(log)
			df_log.to_csv('log_umkm.csv')
			save_every = save_every_bak
			print('						umkm.csv')
df_umkm = pd.DataFrame.from_dict(list_umkm_id)
df_umkm.to_csv('umkm.csv')
print('						umkm.csv')
print('DONE!')