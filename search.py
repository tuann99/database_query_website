#!/usr/local/bin/python3

import cgi
import jinja2 as j
import re
import mysql.connector

form = cgi.FieldStorage()
search_term = form.getvalue('search_box')

# This line tells the template loader where to search for template files
templateLoader = j.FileSystemLoader( searchpath="./path" )

# This creates your environment and loads a specific template
env = j.Environment(loader=templateLoader)
template = env.get_template('final_html_home.html')

conn = mysql.connector.connect(user='tnguy256', password='C0mpl3x21@',
                               host='localhost', database='tnguy256')

# This is what we'll pass to the template, which needs a list of dict() elements.
sql_query = list()

qry = """
  SELECT d.cell_line_id, d.ic_50, d.drug_target, d.drug_target_pathway, c.cell_line_name, c.tissue_desc_1
  FROM drug d 
     JOIN cell_line c ON d.cell_line_id=c.cell_line_id 
  WHERE d.drug_name like %s;
"""

curs = conn.cursor()
curs.execute(qry, ('%' + search_term + '%',))

for item in curs:
      sql_query.append(item)
      # sentries.append({'uniquename':row[0].decode(), 'product':row[1].decode()})

print("Content-Type: text/html\n\n")
print(template.render(sql_query=sql_query,))

curs.close()
conn.close()