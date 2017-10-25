
# coding: utf-8

# # 01. 데이터 베이스 연결 

# 데이터베이스를 사용하려면 실제 저장된 데이터베이스 파일을 반영하는 Connection 객체를 생성해야함.
# DB 파일에 이미 테이블이 생성되거나 레코드가 입력된 경우 Connection 객체를 통해 조회, 입력 등의 연산이 가능

# sqllite는 디스크 기반의 가벼운 데이터베이스 라이브러리 -> 별도의 서버가 필요하지 않기 떄문에 자원을 적게 사용하며 트랜잭션을 지원하기 때문에 운영체제상 문제가 발새하거나 비정상적으로 전원이 종료되어도 데이터의 무결성을 보장!!

# In[1]:


import sqlite3
con=sqlite3.connect("test.db")


# In[2]:


con


# 'memory:' 라는 키워드를 사용해 메모리상에 DB 파일을 만들 수 있다. 
# 연결이 종료되면 현재까지 작업한 모든 내용이 사라지지만 물리적인 DB 파일에 기록하는 것보다는 연산속도가 빠르다고함ㅁ

# In[4]:


con=sqlite3.connect(":memory:")


# # 02. SQL문 수행 
SQLite API를 사용할때는 다음과 같은 과정을 거침.
1. 커넥션 열기
2. 커서 열기
3. 커서를 이용해 데이터 조회/ 수정 / 추가/ 닫기
4. 커넥션 닫기DBMS와 응용프로그램간에 논리적인 연결을 커넥션 
SQLite는 파일 기반의 DBMS이므로 커넥션 열기를 데이터 베이스 파일 열기라고 생각하면 됨.

커서는 실질적으로 SQL을 실행하고 그 결과에 대해 후속작업을 할 수 있게 해주는 객체 
# In[6]:


import sqlite3
con=sqlite3.connect(":memory:")


# In[7]:


cur=con.cursor() #커서 객체 생성


# In[8]:


cur.execute("CREATE TABLE PHONEBOOK(Name CHAR(32) , PhoneNum CHAR(32) PRIMARY KEY);")


# In[9]:


cur.execute("INSERT INTO PHONEBOOK VALUES('hyeju','010-3007-8665');")


# Cursor.execute() 함수에서 sql 구문에서 인자를 채워질 부분을 ?로 표시하고 해당하는 인자를 시퀀스 객체로 전달할 수도 있음.

# In[19]:


name="hyeju"
phonenumber='010-3007-8675'


# In[20]:


cur.execute("INSERT INTO PHONEBOOK Values(?,?);",(name,phonenumber))


# In[23]:


#더 간단하게 다음과 같이도!
cur.execute("INSERT INTO PHONEBOOK Values(:inputname,:inputnum);",{"inputnum":phonenumber,"inputname":name})


# In[29]:


#2개의 레코드를 연속적으로 입력하려면?
datalist={('hyeju','010-3007-8369'),('hyesu','010-1234-3778')}
cur.executemany("INSERT INTO PHONEBOOK Values(?,?);",datalist)


# In[64]:


##script.txt 파일에 저장된 sql 구문을 읽어서 일괄 수행하는 예제

import sqlite3

con = sqlite3.connect(":memory:")

with open('script.txt') as f:
    SQLScript = f.read()

cur = con.cursor()
cur.executescript(SQLScript)


# In[31]:


f=open('script.txt','r')
f.read()


# 
# # 03.레코드 조회

# In[38]:


cur.execute("SELECT * FROM PHONEBOOK")


# In[39]:


for row in cur:
    print(row)


# Cursor.fetchone()은 조회된 결과 집합으로부터 row 객체를 가져옴  Cursor.fetchmany(n)는 조회된 결과에서 인자로 입력된 n개 만큼 row를 리스트 형태로 반환함

# In[40]:


cur.execute("SELECT * FROM PHONEBOOK")


# In[41]:


cur.fetchone()


# In[42]:


cur.fetchmany(2)

cursor.fetchall()은 fetchmany()와 유사하게 조회된 결과의 다음 row 부터 모든 레코드를 리스트 형태로 반환함
# In[44]:


cur.execute("SELECT * FROM PHONEBOOK")


# In[45]:


cur.fetchone()


# In[46]:


cur.fetchall()


# # 04. 트랜잭션 처리

# In[49]:


import sqlite3
con = sqlite3.connect("./test.db")
cur = con.cursor()
#cur.execute("DROP TABLE PHONEBOOK;")
cur.execute("CREATE TABLE PHONEBOOK(Name text, PhoneNum text);")
cur.execute("INSERT INTO PHONEBOOK VALUES('hyeju', '010-3007-8665');")
cur.execute("SELECT * FROM PHONEBOOK;")
print(cur.fetchall())


# In[50]:


import sqlite3
con = sqlite3.connect("./test.db")
cur = con.cursor()
cur.execute("SELECT * FROM PHONEBOOK;")
print(cur.fetchall())


# In[51]:


##빈 셀이 나타나는 것은 트랜잭션 처리와 연관이 있음. 


# 트랜잭션 처리란??
# -->p310

# In[57]:


import sqlite3
con = sqlite3.connect("./commit.db")
cur = con.cursor()
cur.execute("CREATE TABLE PhoneBook(Name text, PhoneNum text);")
cur.execute("INSERT INTO PhoneBook VALUES('hyeju', '010-3007-8665');")
con.commit()


# In[58]:


import sqlite3
con = sqlite3.connect("./commit.db")
cur = con.cursor()
cur.execute("SELECT * FROM PhoneBook;")
print(cur.fetchall())


# In[59]:


#자동으로 commit 되게 설정
con.isolation_level=None


# # 05. 레코드 정렬과 사용자 정렬 함수

# In[ ]:


#ORDER BY를 이용해 Name을 정렬


# In[72]:


cur.execute("SELECT * FROM Phonebook ORDER BY Name")


# In[73]:


[r for r in cur]


# In[80]:


cur.execute("SELECT * FROM Phonebook ORDER BY Name DESC")
[r for r in cur]


# In[81]:


cur.execute("INSERT INTO Phonebook VALUES('hyesu','010-4127-3004');")
cur.execute("SELECT * FROM Phonebook ORDER BY Name")
[r[0] for r in cur]


# In[82]:


#대소문자 구분 없이 정렬하는 함수!
def OrderFunc(a,b):
    s1=a.upper()
    s2=b.upper()
    return (s1>s2)-(s1<s2)


# In[83]:


con.create_collation('myordering',OrderFunc)
#SQL구문에서 호출할 이름과 함수 등록


# In[84]:


cur.execute("SELECT Name FROM Phonebook ORDER BY Name COLLATE myordering")
[r[0] for r in cur]


# # 06. SQLite3 내장 집계 함수
abs(x) 절대값 반환
length(x) 문자열 길이 반환
lower(x) 소문자 반환
upper(x) 대문자 반환
min(x,...) 최소 max(x,..) 최대
random(*) 임의의 정수
count(x) 필드인자가 NULL값이 아닌 튜플의 개수 반환
count(*) 튜플의 개수 반환
sum(x) 필드 인자의 합 반환
# In[85]:


import sqlite3
con = sqlite3.connect(":memory:")
cur = con.cursor()

cur.execute("CREATE TABLE PhoneBook(Name text, Age integer, Sex text);")
list = (('Hyeju', 23,'F'),('Seoho',28,'M'), ('Dakung',24,'F'), ('Gayoung',25,'F'))
cur.executemany("INSERT INTO PhoneBook VALUES(?, ?, ?);", list)

cur.execute("SELECT length(Name), upper(Name), lower(Name) FROM PhoneBook")
print("== length(), upper(), lower() ==")
print([r for r in cur])

cur.execute("SELECT max(Age), min(Age), sum(Age) FROM PhoneBook")
print("== max(), min(), sum() ==")
print([r for r in cur])

cur.execute("SELECT count(*), random(*) FROM PhoneBook")
print("== count(*), random(*) ==")
print([r for r in cur]) 

cur.execute("SELECT count(*) FROM PhoneBook GROUP BY Sex")
print("== count(*)")
print([r for r in cur]) 


# # 07. 사용자정의 집계 함수

# 지원하는 내장 집계함수만으로 부족할때 사용자가 직접 클래스를 작성해 등록할 수 있음

# In[ ]:


class Average:
    def __init__(self):
        self.sum = 0  ##sum. cnt 초기화
        self.cnt = 0

    def step(self, value):
        self.sum += value #입력된 값을 sum에 더하고 cnt를 증가
        self.cnt += 1

    def finalize(self):
        return self.sum / self.cnt #평균을 반환


# 이렇게 정의된 클래스는 Connection.create_aggregate() 메서드를 호출해 DB에 등록해야 쓸 수 있음. step() 함수에 전달될 인자의 개수, 클래스 명을 순차적으로 인자로 입력받음

# In[ ]:


con.create_aggregate("avg",1,Average) #Average 클레스를 사용자정의 집계함수로 등록

cur.execute("SELECT avg(Age) FROM Phonebook")
print(cur.fetchone()[0])


# # 08. 자료형
SQLite3 자료형 / 파이썬 자료형
NULL / None
INTEGER / int
REAL / float #약 9개의 유효 숫자가 포함된 단정밀도 부동 소수점 수가 저장
TEXT / str, bytes
BLOB / buffer #2진 데이터 
# In[ ]:


con = sqlite3.connect(":memory:")
cur = con.cursor()

cur.execute("CREATE TABLE PhoneBook(Name text, Age integer, Money REAL);") # SQLite3 자료형으로 테이블 생성
cur.execute("CREATE TABLE PhoneBook2(Name str, Age int, Money float);") # 파이썬 자료형으로 테이블 생성

cur.execute("INSERT INTO Phonebook VALUES('혜주',23,10000000000000000.123)")
cur.execute("INSERT INTO Phonebook2 VALUES('혜주',23,10000000000000000.123)")


# # 09. 사용자정의 자료형

# SQLite3는 5개의 자료형으로 되어있는데 이 5개로 부족할 수 있음. 그럴 때 사용자정의 자료형을 직접 클래스 객체로 DB에 입력할 수 있음

# In[ ]:


#2차원의 자표를 나타내는 Point 클래스

class Point(object):
    def __init__(self, x, y):
        self.x, self.y = x, y

    def __repr__(self):
        return "Point(%f, %f)" % (self.x, self.y)


# 정의된 클래스를 SQLite3에서 입력/ 조회하려면 변환함수를 작성해야함. 왜? SQLite3는 5개의 기본 자료형만을 입력받을 수 있어서

# In[ ]:


#PointAdapter() 는 사용자정의 자료형을 SQLite3에서 사용 가능한 형태로 반환하는 함수 

def PointAdapter(point):
    return "%f:%f" % (point.x, point.y)

#PointConverter()는 SQLite3에서 조회된 결과를 클래스 객체 형태로 반환하는 함수

def PointConverter(s):
    x, y = list(map(float, s.decode().split(":")))
    return Point(x, y)


# ##작성된 함수는 db에 등록해야 사용할 수 있음
# 
# sqlite3.register_adapter(파이썬 자료형, 변환함수)
# 
# sqlite3.register_converter(SQLite3 자료형, 변환함수)

# In[ ]:


sqlite3.register_adapter(Point, PointAdapter) #클래스 이름과 변환함수 등록


# In[ ]:


sqlite3.register_converter("point", PointConverter) #SQL구문에서 사용할 자료형 이름과 변환함수 등록


# In[ ]:


p = Point(4, -3.2)
p2 = Point(-1.4, 6.2)

con = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES) #암묵적으로 선언된 자료형으로 조회하도록 설정
cur = con.cursor()
cur.execute("create table test(p point)") #point 자료형으로 테이블 생성
cur.execute("insert into test values (?)", (p, )) #point 레코드 입력
cur.execute("insert into test(p) values (?)", (p2,))

cur.execute("select p from test")
print([r[0] for r in cur])
cur.close()
con.close()


# # 10. 데이터베이스 덤프 만들기

# In[ ]:


##SQL 문으로 만들어서 쓰고 싶을때 다음과 같은 코드 쓰면됨


# In[ ]:


import sqlite3
con = sqlite3.connect(":memory:")
cur = con.cursor()

cur.execute("CREATE TABLE PhoneBook(Name text, PhoneNum text);")
cur.execute("INSERT INTO PhoneBook VALUES('Derick', '010-1234-5678');")
list = (('Tom', '010-543-5432'), ('DSP', '010-123-1234'))
cur.executemany("INSERT INTO PhoneBook VALUES(?, ?);", list)

for l in con.iterdump():
    print(l)


# In[ ]:





# In[ ]:




