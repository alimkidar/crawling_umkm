import pandas as pd
import numpy as np
import grequests, time
from bs4 import BeautifulSoup as soup
import requests


def multi_req(list_):
	rs = (grequests.get(u, timeout=60) for u in list_)
	x = grequests.map(rs)
	return x

def get_info(r, data_):
	page_html = r.content
	page_soup = soup(page_html, "html.parser")
	containers = page_soup.findAll('tr')
	data_.append({
		'nama': containers[1].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Nama Usaha',''),
		'nomor_surat_izin': containers[2].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Nomor Surat Ijin',''),
		'tgl_mulai': containers[3].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Tanggal Mulai Usaha',''),
		'npwp': containers[4].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('NPWP',''),
		'status_usaha': containers[5].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Status Usaha',''),
		'alamat': containers[6].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Alamat',''),
		'kelurahan/desa': containers[7].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Kelurahan / Desa',''),
		'kecamatan' : containers[8].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Kecamatan',''),
		'kabupaten' : containers[9].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Kabupaten',''),
		'provinsi' : containers[10].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Provinsi',''),
		'kode_pos' : containers[11].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Kodepos',''),
		'no_telp' : containers[12].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('No Telpon',''),
		'no_telp_kantor' : containers[13].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('No Telpon Kantor',''),
		'faksimili' : containers[14].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Faksimili',''),
		'email' : containers[15].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Email',''),
		'website' : containers[16].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Website',''),
		'bentuk_usaha' : containers[17].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Bentuk Usaha',''),
		'sektor_usaha' : containers[18].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Sektor Usaha',''),
		'skala_usaha' : containers[19].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Skala Usaha',''),
		'jml_tenaga_kerja_pria': containers[21].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Jumlah Tenaga Kerja Pria',''),
		'jml_tenaga_kerja_wanita': containers[22].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Jumlah Tenaga Kerja Wanita',''),
		'jml_tenaga_kerja': containers[23].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Jumlah Tenaga Kerja',''),
		'jml_karyawan_pria': containers[24].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Jumlah Karyawan Pria',''),
		'jml_karyawan_wanita': containers[25].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Jumlah Karyawan Wanita',''),
		'jml_karyawan': containers[26].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Total Karyawan',''),
		'id_umkm': containers[28].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace(' UMKM',''),
		'grade': containers[29].text.strip().replace(',','|').replace('\n','|').replace('\t','|').replace('Grade',''),
		})
	return data_


input_csv = input('Masukan file csv... ')

df = pd.read_csv(input_csv)
df.columns = ['x','y']
df['z'] = 'http://umkm.depkop.go.id/Detail.aspx?KoperasiId=' + df['y'].astype(str)

urls = df['z'].tolist()
n = 10

z = [urls[i * n:(i + 1) * n] for i in range((len(urls) + n - 1) // n )]

data = []
hit = 0

output_name = 'detil_' + input_csv
print('start looping.')
try:
	for i in z:
		urls_temp = []
		urls_temp = urls_temp + i
		hit_r = 1
		while len(urls_temp) != 0:
			start_time = time.time()
			print(str(hit_r),'sending requests...')
			hit_r += 1
			x = multi_req(urls_temp)
			elapsed_time = time.time() - start_time
			print(str(hit_r),'done.', str(elapsed_time) + 's. kurang', str(len(urls_temp)), 'urls')
			urls_temp = []
			for r in x:
				url_temp = r.url
				if r.status_code == requests.codes.ok:
					get_info(r, data)
				else:
					urls_temp.append(url_temp)
		
			time.sleep(2)
		hit += 1
		hit_r = 0
		df = pd.DataFrame.from_dict(data)
		df = df[['nama','nomor_surat_izin','tgl_mulai','npwp','status_usaha','alamat','kelurahan/desa','kecamatan','kabupaten','provinsi','kode_pos','no_telp','no_telp_kantor','faksimili','email','website','bentuk_usaha','sektor_usaha','skala_usaha','jml_tenaga_kerja_pria','jml_tenaga_kerja_wanita','jml_tenaga_kerja','jml_karyawan_pria','jml_karyawan_wanita','jml_karyawan','id_umkm','grade']]
		df.to_csv(output_name)
		print(str(hit * len(i)), 'saved as ' + output_name)
except:
	raise
df = pd.DataFrame.from_dict(data)
df = df[['nama','nomor_surat_izin','tgl_mulai','npwp','status_usaha','alamat','kelurahan/desa','kecamatan','kabupaten','provinsi','kode_pos','no_telp','no_telp_kantor','faksimili','email','website','bentuk_usaha','sektor_usaha','skala_usaha','jml_tenaga_kerja_pria','jml_tenaga_kerja_wanita','jml_tenaga_kerja','jml_karyawan_pria','jml_karyawan_wanita','jml_karyawan','id_umkm','grade']]
df.to_csv(output_name)
print(str(hit * len(i)), 'saved as ' + output_name)
