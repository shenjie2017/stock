#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 定时 爬取每日股票行情数据;
# 股票数据内容：
from bs4 import BeautifulSoup
import pymysql
import re
import json
import requests
import traceback


def getHTMLText(url, code="utf-8"):
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        r.encoding = code
        return r.text
    except:
        return ""


def getStockList(stock_list=[]):
    # 第一步, 通过中财网获取股票数据
    sheets = [11, 12, 13, 14]
    url = 'http://quote.cfi.cn/stockList.aspx?t='
    for sheet in sheets:
        html = getHTMLText(url + str(sheet))
        soup = BeautifulSoup(html, 'html.parser')
        a = soup.find_all('a')
        for i in a:
            try:
                href = i.attrs['href']
                stock_list.append(re.findall(r"\d{6}.html", href)[0])
            except:
                continue
    return stock_list


def getDBConf(conf_filename="databases_conf.json", encoding="utf-8"):
    with open(conf_filename, encoding=encoding) as f:
        db_conf = json.load(f)

    return db_conf


# db连接配置
def getDBConnect(db_conf):
    return pymysql.connect(host=db_conf["host"], user=db_conf["user"], password=db_conf["password"],
                           db=db_conf["database"], port=db_conf["port"], charset=db_conf["charset"])


def formatData(infoDict):
    for item in infoDict:
        if item in ['股票名称', '股票代码', '日期', '所属行业']:
            continue
        value = infoDict[item]
        value = re.sub(r'[^-?\d+.?\d*]|[-]{2}', '', value)
        if value == '' or value is None:
            value = 'null'
        infoDict[item] = value

    return infoDict


def createDataBase(db):
    sql = 'CREATE DATABASE IF NOT EXISTS stock DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;'
    db.cursor().execute(sql)
    db.commit()


def createTable(db):
    createDataBase(db)
    sql = '''
            CREATE TABLE IF NOT EXISTS stock.stock
        (
            日期   VARCHAR(10) COMMENT '日期',
            股票代码 VARCHAR(10) COMMENT '股票代码',
            股票名称 VARCHAR(50) COMMENT '股票名称',
            所属行业 VARCHAR(100) COMMENT '所属行业',
            行业平均市盈率 DECIMAL(19, 2) COMMENT '行业平均市盈率',
            行业扣除后平均市盈率 DECIMAL(19, 2) COMMENT '行业扣除后平均市盈率',
            股价   DECIMAL(19, 2) COMMENT '股价',
            股价波动 DECIMAL(19, 2) COMMENT '股价波动',
            股价波幅 DECIMAL(19, 2) COMMENT '股价波幅',
            今开   DECIMAL(19, 2) COMMENT '今开',
            昨收   DECIMAL(19, 2) COMMENT '昨收',
            最高   DECIMAL(19, 4) COMMENT '最高',
            最低   DECIMAL(19, 2) COMMENT '最低',
            振幅   DECIMAL(19, 2) COMMENT '振幅',
            换手率  DECIMAL(19, 2) COMMENT '换手率',
            成交量  DECIMAL(19, 2) COMMENT '成交量',
            成交额  DECIMAL(19, 4) COMMENT '成交额',
            市盈率  DECIMAL(19, 2) COMMENT '市盈率',
            市净率  DECIMAL(19, 2) COMMENT '市净率'
        ) COMMENT '股票行情表'
            DEFAULT CHARACTER SET = utf8;'''

    db.cursor().execute(sql)
    db.commit()


def getStockInfo(stock_list):
    ready_count = 0
    all_count = len(stock_list)
    db = getDBConnect(getDBConf())
    createTable(db)

    for stock_code in stock_list:
        url = "http://quote.cfi.cn/" + str(stock_code)
        html = getHTMLText(url)
        # print(html)
        try:
            if html == '':
                continue
            # 每个股票存为字典，数据处理较麻烦，有些数据有“杂音”，需单独给出if判断，或在正则中约束
            infoDict = {}
            soup = BeautifulSoup(html, "html.parser")
            stockInfo = soup.find('div', attrs={'id': 'act_quote'}).find('table').find('table')
            # print(stockInfo)

            name = stockInfo.find('div', attrs={'class': 'Lfont'}).string
            infoDict.update({'股票代码': re.sub(r"\D+", "", name)})
            infoDict.update({'股票名称': re.sub(r"\d+|\(|\)", "", name)})

            price = stockInfo.find('span', attrs={'id': 'last'})
            chg = stockInfo.find('span', attrs={'id': 'chg'})
            hq_time = stockInfo.find('td', attrs={'id': 'hq_time'})
            price = re.sub(r'[^-?\d+.?\d*]', '', price.get_text())
            hq_date = hq_time.get_text().split(' ')[0]

            if hq_date is None or hq_date == '' or hq_date == '--':
                continue
            infoDict.update({'股价': price})
            infoDict.update({'日期': hq_date})
            infoDict.update({'股价波动': chg.contents[0]})
            infoDict.update({'股价波幅': chg.contents[2]})

            stockDetialInfo = stockInfo.find('td', attrs={'class': 'Rlist'})
            td = stockDetialInfo.find_all("td")
            for item in td:
                text = item.get_text()
                if (text == "业绩预告"):
                    key = "业绩预告"
                    real_val = "业绩预告"
                else:
                    text_split = re.split(':|：', text)  # 网站程序员分号用了中文和英文两种……
                    key = text_split[0]
                    val = text_split[1]
                    real_val = re.search(r'(-?\d+.?\d*[%|手|万|元]?)|(--)|(正无穷大)', val).group(0)

                infoDict[key] = real_val

            industryInfo = stockInfo.find(lambda e: e.name == 'td' and '所属行业' in e.text).find(lambda e: e != 'span')
            infoDict['所属行业'] = industryInfo.next
            infoDict['行业平均市盈率'] = re.split(':|：', industryInfo.next.next.next)[1]
            infoDict['行业扣除后平均市盈率'] = re.split(':|：', industryInfo.next.next.next.next.next)[1]

            # print(infoDict)
            # {'股票代码': '600004', '股票名称': '白云机场', '股价': '15.63', '日期': '2020-06-16', '股价波动': '0.42', '股价波幅': '2.76%',
            #  '今开': '15.40', '最高': '16.17', '振幅': '5.19%', '换手率': '0.63%', '昨收': '15.21', '最低': '15.38',
            #  '成交量': '129626手', '成交额': '20299.38万', '市盈率': '--', '扣除后市盈率': '--', '市净率': '1.93',
            #  '2020-03-31 每股收益': '-0.03元'}

            infoDict = formatData(infoDict)
            # print(infoDict)

            valueString = "INSERT INTO stock.stock(日期,股票代码,股票名称,所属行业,行业平均市盈率,行业扣除后平均市盈率,股价,股价波动,股价波幅,今开,昨收,最高,最低,振幅,换手率,成交量,成交额,市盈率,市净率) " \
                          "values ('{}','{}','{}','{}',{},{},{},{},{},{},{},{},{},{},{},{},{},{},{});" \
                .format(infoDict.get('日期', 'null'), infoDict.get('股票代码', 'null'), infoDict.get('股票名称', 'null'),
                        infoDict.get('所属行业', 'null'), infoDict.get('行业平均市盈率', 'null'),
                        infoDict.get('行业扣除后平均市盈率', 'null'),
                        infoDict.get('股价', 'null'), infoDict.get('股价波动', 'null'), infoDict.get('股价波幅', 'null'),
                        infoDict.get('今开', 'null'), infoDict.get('昨收', 'null'),
                        infoDict.get('最高', 'null'), infoDict.get('最低', 'null'),
                        infoDict.get('振幅', 'null'), infoDict.get('换手率', 'null'), infoDict.get('成交量', 'null'),
                        infoDict.get('成交额', 'null'), infoDict.get('市盈率', 'null'),
                        infoDict.get('市净率', 'null'))

            db.cursor().execute(valueString)

            ready_count += 1
            print('\r当前第{0:}个,共{1:}个.stock_code:{2:}'.format(ready_count, all_count, stock_code))  # 打印进度
            if ready_count % 100 == 0:
                db.commit()
        except AttributeError as e:
            ready_count += 1
            print(
                '\r当前第{0:}个,共{1:}个.stock_code:{2:} .exception msg: 解析html异常'.format(ready_count, all_count, stock_code))
            traceback.print_exc()
    db.commit()


if __name__ == '__main__':
    stock_list = getStockList()
    getStockInfo(stock_list)
