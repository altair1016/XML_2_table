import xml.etree.ElementTree as ET

filename_in = 'sample.xml'
filename_out = 'fout.sql'


#ddl parameters
jar_path = 'hdfs://path/jar'
hdfs_path = 'hdfs://path/project_name/'
jar_path = 'serde_position/hivexmlserde-1.0.5.3.jar'
db_name = 'project'
table_ext = 'book_ext'
table = 'book'
drop_table = 'project.book_ext'
create_ext = "project.book_ext"
location = "'hdfs_project/tablename'"
additional_sql ="drop table if exists project.book;\n" \
            "create table project.book as \n" \
            "select *, current_timestamp as dta_process \n" \
            "from project.book_ext_ext where 2=1;"


head_of_ddl = "set jar_hdfsfolder="+ jar_path +";" \
              '\n' + "set proj_hdfsfolder="+hdfs_path+";" \
              '\n' + "--add jar  "+jar_path+";" \
              '\n' + "create schema " + db_name + ";" \
              '\n' + "set db_name="+db_name+";" \
              '\n' + "set table_name_ext="+table_ext+";" \
              '\n' + "set table_name="+table+";" \
              '\n' + "drop table if exists "+drop_table+";" \
              '\n\n\n' + " CREATE EXTERNAL TABLE "+create_ext+" ("



tree = ET.parse(filename_in)
root = tree.getroot()
file_out = open(filename_out, 'w')




if len(root.attrib) > 0:
    field_list = "\n\t" + str(root.tag) + "_" + str(list(root.attrib)[0]) + "\t string,"
else:
    field_list = "\n\t" + str(root.tag) + "\t string,"



for node in root:
        field_list += "\n\t" + str(node.tag) + "\t string,"

head_of_ddl += field_list[:-1] + "\n)" \
                                 "\nROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe' WITH SERDEPROPERTIES (" \
                                 "\n"

path_xml = '"column.xpath.'
attr= ""
serde_prop =""
if len(root.attrib) > 0:
    serde_prop = path_xml + str(root.tag) + "_" + str(list(root.attrib)[0]) + "\"=\"/" + root.tag + "/@" + list(root.attrib)[0] +  "\","
    attr = str(list(root.attrib)[0])
else:
    serde_prop = path_xml + str(root.tag) + "\"=\"" +  + root.tag + "text()\","


for node in root:
    serde_prop += '\n'
    if len(node.attrib) > 0:
        serde_prop += path_xml + str(node.tag) + "\"=\"/" + root.tag + "/"+ str(node.tag) + "/@" + list(root.attrib)[0] + "\","
    else:
        serde_prop += path_xml + str(node.tag) + "\"=\"/" + root.tag + "/"+ str(node.tag) + "/text()\","
serde_prop = serde_prop[:-1]
tail_code = ")\nSTORED AS INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'" \
            +"\n"+"OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'" \
            +"\n"+"LOCATION " + location \
            +"\n"+ "TBLPROPERTIES (\"xmlinput.start\"=\"<" + root.tag + " " + attr+"\",\"xmlinput.end\"=\"</"+ root.tag+">\" );\n\n " \
            + additional_sql

file_out.write(head_of_ddl + serde_prop + tail_code)
file_out.close()