set jar_hdfsfolder=serde_position/hivexmlserde-1.0.5.3.jar;
set proj_hdfsfolder=hdfs://path/project_name/;
--add jar  serde_position/hivexmlserde-1.0.5.3.jar;
set db_name=project;
set table_name_ext=book_ext;
set table_name=book;
drop table if exists project.book_ext;


 CREATE EXTERNAL TABLE project.book_ext (
	book_id	 string,
	author	 string,
	title	 string,
	genre	 string,
	price	 string,
	publish_date	 string,
	description	 string
)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe' WITH SERDEPROPERTIES (
"column.xpath.book_id"="/book/@id",
"column.xpath.author"="/book/author/text()",
"column.xpath.title"="/book/title/text()",
"column.xpath.genre"="/book/genre/text()",
"column.xpath.price"="/book/price/text()",
"column.xpath.publish_date"="/book/publish_date/text()",
"column.xpath.description"="/book/description/text()")
STORED AS INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION 'hdfs_project/tablename'
TBLPROPERTIES ("xmlinput.start"="<book id","xmlinput.end"="</book>" );

 drop table if exists project.book;
create table project.book as 
select *, current_timestamp() as dta_process 
from project.book_ext_ext where 2=1;
