from bs4 import BeautifulSoup
import requests
import mysql.connector
from datetime import datetime
from credentials import Credentials


creds = Credentials()
def insert():
    try:
        print("Starting...")
        db = mysql.connector.connect(
            host=creds.host,
            user=creds.user,
            password=creds.password,
            database=creds.database,
            port=3306,
            auth_plugin='caching_sha2_password'
        )
        cursor = db.cursor()


        page = 9999
        parse_url = str.format('https://bbs.io-tech.fi/threads/yleinen-verkkokauppojen-alennuskoodit.907/page-{}', page)

        headers = { "user-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}

        html_text = requests.get(parse_url,headers=headers).text
        soup = BeautifulSoup(html_text, 'html.parser')


        max_number = soup.find("li", attrs={'class':'pageNav-page--current'}).get_text()
        for i in range(1, int(max_number)+1):
            parse_url = str.format('https://bbs.io-tech.fi/threads/yleinen-verkkokauppojen-alennuskoodit.907/page-{}', i)

            headers = { "user-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36'}

            html_text = requests.get(parse_url,headers=headers).text
            soup = BeautifulSoup(html_text, 'html.parser')
            print(str.format("Posts, page {}. Total posts: {}", i, len(soup.find_all("article", {'class':'message--post'}))))
            for item in soup.find_all(class_="message--post"):
                to_split = str.split(item.get('id'),"-")
                post_id = to_split[2]
                print(post_id, end=",")
                dt = item.find("time", attrs={"class":"u-dt"})
                dt_timestamp = int(dt.get("data-time"))
                dt_date = datetime.fromtimestamp(dt_timestamp)
                print(datetime.fromtimestamp(dt_timestamp))
                print("")

                # Search for a message with the post id
                get_post_query = "SELECT * FROM message WHERE post_id = %s"
                get_post_val = (post_id, )
                cursor.execute(get_post_query,get_post_val)
                get_post_result = cursor.fetchall()
                # If message does not exist
                if len(get_post_result) == 0:
                    # check if page has any messages
                    message_amount_query = "SELECT message_amount FROM amount WHERE page = %s"
                    message_amount_val = (i, )
                    cursor.execute(message_amount_query, message_amount_val)
                    message_amount_result = cursor.fetchall()
                    
                    # If no messages, add one
                    if len(message_amount_result) == 0:
                        amount_zero_insert = "INSERT INTO amount (page, message_amount) values (%s, %s)"
                        amount_zero_val = (i, 1)
                        cursor.execute(amount_zero_insert, amount_zero_val)
                        db.commit()
                        print("inserted")
                    elif len(message_amount_result) > 0:
                    # There are messages, so just add 1 to the count
                        amount_int = len(soup.find_all(class_="message--post"))
                        print(amount_int)
                        update_amount_query = "UPDATE amount SET message_amount = %s WHERE page = %s"
                        update_amount_val = (amount_int, i)
                        print(update_amount_val)
                        cursor.execute(update_amount_query, update_amount_val)
                        db.commit()
                        print("updated")
                    save_message_query = "INSERT INTO message (post_id, timestamp, page, added) values (%s, %s, %s, %s)"
                    save_message_val = (post_id, dt_date, i, 0)
                    cursor.execute(save_message_query, save_message_val)
                    db.commit()
                    print("post added to db")
    except Exception as e:
        print("An error occured while inserting the posts into db: ", e)


if __name__ == "__main__":
    insert()
