-- drop table emp;
-- -- external table ensures data will not be dropped from locaton it was stored
-- create external table emp (empno INT, ename STRING, job STRING, 
-- mgr INT, hiredate DATE, sal FLOAT, deptno INT)
-- row format delimited
-- fields terminated by '\t'
-- lines terminated by '\n'
-- stored as textfile
-- location '/user/bitnami/ex_data/emp_dept_2';
-- -- location avoid mv to /user/hive/warehouse
-- load data inpath "/user/bitnami/ex_data/emp_dept/emp.csv" into table emp;


create table emp (empno INT, ename STRING, job STRING, 
mgr INT, hiredate DATE, sal FLOAT, deptno INT)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

-- load data local inpath "" overwrite table emp;
load data local inpath "/home/bitnami/Desktop/emp_dept/emp.csv" into table emp;
select * from emp limit 10;
-- drop table emp;

-- if location is same directory as original file, original file gets renamed
-- since it mv into same directory into emp_copy_1, 
-- and that causes all files inpath to load into one table
-- BEST SOLUTION: load data LOCAL (creates copy rather than move)
-- and let hive do the deleting in its default directory
-- /user/hive/warehouse when table is dropped to avoid file_copy_1 problems


create table dept (deptno INT, dname STRING, loc STRING)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

load data local inpath "/home/bitnami/Desktop/emp_dept/dept.csv" into table dept;
select * from dept limit 10;


create table salgrade (grade INT, losal INT, hisal INT)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

load data local inpath "/home/bitnami/Desktop/emp_dept/salgrade.csv" into table salgrade;
select * from salgrade limit 10;



-- 1. [E] (4 points): Smith’s employment date
select hiredate
from emp
where ename == "SMITH";
-- hive (default)> select hiredate
--               > from emp
--               > where ename == "SMITH";
-- OK
-- 1980-12-17
-- Time taken: 0.513 seconds, Fetched: 1 row(s)


-- 2. [E] (4 points): Ford’s job title
select job
from emp
where ename == "FORD";
-- hive (default)> select job
--               > from emp
--               > where ename == "FORD";
-- OK
-- ANALYST
-- Time taken: 0.07 seconds, Fetched: 1 row(s)


-- 3. [E] (4 points): The first employee (by the hiredate)
select ename,hiredate
from emp
sort by hiredate ASC
limit 1;
-- Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.33 sec   HDFS Read: 801 HDFS Write: 122 SUCCESS
-- Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 5.9 sec   HDFS Read: 524 HDFS Write: 17 SUCCESS
-- Total MapReduce CPU Time Spent: 9 seconds 230 msec
-- OK
-- SMITH	1980-12-17
-- Time taken: 69.54 seconds, Fetched: 1 row(s)


-- 4. [E] (4 points): The number of employees in each department
--  HIVE advantage: any error prompt on start rather than PIG lazy evaluation
select d.dname, count(1) as ct
from emp e join dept d
on (e.deptno = d.deptno)
group by d.dname
sort by ct DESC;
-- MapReduce Jobs Launched: 
-- Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 3.89 sec   HDFS Read: 801 HDFS Write: 176 SUCCESS
-- Stage-Stage-3: Map: 1  Reduce: 1   Cumulative CPU: 2.8 sec   HDFS Read: 578 HDFS Write: 32 SUCCESS
-- Total MapReduce CPU Time Spent: 6 seconds 690 msec
-- OK
-- SALES	6
-- RESEARCH	5
-- ACCOUNTING	3
-- Time taken: 86.622 seconds, Fetched: 3 row(s)


-- 5. [E] (4 points): The number of employees in each city
select d.loc, count(1) as ct
from emp e join dept d
on (e.deptno = d.deptno)
group by d.loc
sort by ct DESC;
-- MapReduce Jobs Launched: 
-- Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 4.15 sec   HDFS Read: 801 HDFS Write: 174 SUCCESS
-- Stage-Stage-3: Map: 1  Reduce: 1   Cumulative CPU: 4.35 sec   HDFS Read: 576 HDFS Write: 30 SUCCESS
-- Total MapReduce CPU Time Spent: 8 seconds 500 msec
-- OK
-- CHICAGO	6
-- DALLAS	5
-- NEW YORK	3
-- Time taken: 83.208 seconds, Fetched: 3 row(s)



-- 6. [E] (4 points): The average salary in each city
select d.loc, avg(e.sal) as average_sal
from emp e join dept d
on (e.deptno = d.deptno)
group by d.loc
sort by average_sal DESC;
-- MapReduce Jobs Launched: 
-- Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 4.61 sec   HDFS Read: 801 HDFS Write: 195 SUCCESS
-- Stage-Stage-3: Map: 1  Reduce: 1   Cumulative CPU: 4.91 sec   HDFS Read: 597 HDFS Write: 69 SUCCESS
-- Total MapReduce CPU Time Spent: 9 seconds 520 msec
-- OK
-- NEW YORK	2916.6666666666665
-- DALLAS	2175.0
-- CHICAGO	1566.6666666666667
-- Time taken: 219.709 seconds, Fetched: 3 row(s)



-- 7. [E] (4 points): The highest paid employee in each department
select t1.ename, t2.dname, t1.sal
from emp t1
inner join
(select d.dname, max(e.sal) as highest_sal
from emp e join dept d
on (e.deptno = d.deptno)
group by d.dname) t2
on t1.sal = t2.highest_sal
order by t1.ename;
-- MapReduce Jobs Launched: 
-- Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 4.92 sec   HDFS Read: 801 HDFS Write: 185 SUCCESS
-- Stage-Stage-4: Map: 1  Reduce: 1   Cumulative CPU: 3.67 sec   HDFS Read: 587 HDFS Write: 85 SUCCESS
-- Total MapReduce CPU Time Spent: 8 seconds 590 msec
-- OK
-- BLAKE	SALES	2850.0
-- FORD	RESEARCH	3000.0
-- KING	ACCOUNTING	5000.0
-- SCOTT	RESEARCH	3000.0
-- Time taken: 118.144 seconds, Fetched: 4 row(s)



-- 8. [D] (4 points): Managers whose subordinates have at least one subordinate
select boss.ename, boss.empno
from emp boss
inner join
(select worker.mgr as worker_boss, worker.empno, worker_worker.empno
from emp worker inner join emp worker_worker
on worker_worker.mgr = worker.empno) as ww
on boss.empno = ww.worker_boss;
--(KING, 7839)
--(JONES, 7655)


-- 9. [D] (4 points): The number of employees for each hiring year
select year(e.hiredate) as hireYr, count(1)
from emp e
group by year(e.hiredate);
-- MapReduce Jobs Launched: 
-- Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.87 sec   HDFS Read: 801 HDFS Write: 29 SUCCESS
-- Total MapReduce CPU Time Spent: 3 seconds 870 msec
-- OK
-- 1980	1
-- 1981	10
-- 1987	2
-- 1991	1
-- Time taken: 33.592 seconds, Fetched: 4 row(s)


-- 10. [D] (4 points): The pay grade of each employee
select e.ename, e.sal, s.grade, s.losal, s.hisal
from emp e
cross join salgrade s
where (s.hisal >= e.sal and s.losal<=e.sal);
-- MapReduce Jobs Launched: 
-- Stage-Stage-3: Map: 1   Cumulative CPU: 4.26 sec   HDFS Read: 801 HDFS Write: 345 SUCCESS
-- Total MapReduce CPU Time Spent: 4 seconds 260 msec
-- OK
-- SMITH	800.0	1	700	1200
-- ALLEN	1600.0	3	1401	2000
-- WARD	1250.0	2	1201	1400
-- JONES	2975.0	4	2001	3000
-- MARTIN	1250.0	2	1201	1400
-- BLAKE	2850.0	4	2001	3000
-- CLARK	2450.0	4	2001	3000
-- SCOTT	3000.0	4	2001	3000
-- KING	5000.0	5	3001	9000
-- TURNER	1500.0	3	1401	2000
-- ADAMS	1100.0	1	700	1200
-- JAMES	950.0	1	700	1200
-- FORD	3000.0	4	2001	3000
-- MILLER	1300.0	2	1201	1400
-- Time taken: 60.257 seconds, Fetched: 14 row(s)

