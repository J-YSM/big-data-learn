emp = load 'ex_data/emp_dept/emp.csv' as (empno:int, ename:chararray, job:chararray, mgr:int, hiredate:datetime, sal:float, deptno: int);
dept = load 'ex_data/emp_dept/dept.csv' as (deptno:int, dname:chararray, loc: chararray);
salgrade = load 'ex_data/emp_dept/salgrade.csv' as (grade:int,losal:int, hisal:int);

--illustrate emp;
-------------------------------------------------------
--| emp     | empno:int    | ename:chararray    | job:chararray    | mgr:int    | hiredate:datetime    | sal:float    | deptno:int    | 
---------------------------------------------------------------------------------------------------------------------------------------
--|         | 7782         | CLARK              | MANAGER          | 7839       | 1981-06-09           | 2450         | 10            | 
---------------------------------------------------------------------------------------------------------------------------------------

--illustrate dept;
----------------------------------------------------------------------
--| dept     | deptno:int    | dname:chararray    | loc:chararray    | 
----------------------------------------------------------------------
--|          | 10            | ACCOUNTING         | NEW YORK         | 
----------------------------------------------------------------------

--illustrate salgrade;
---------------------------------------------------------------
--| salgrade     | grade:int    | losal:int    | hisal:int    | 
---------------------------------------------------------------
--|              | 3            | 1401         | 2000         | 
---------------------------------------------------------------


--1. [E] (4 points): Smith’s employment date
a = filter emp by ename=='SMITH';
a = foreach a generate ename, hiredate;
dump a;
--(SMITH,1980-12-17T00:00:00.000+08:00)

--2. [E] (4 points): Ford’s job title
a = filter emp by ename=='FORD';
a = foreach a generate ename, job;
dump a;
--(FORD,ANALYST)

--3. [E] (4 points): The first employee (by the hiredate)
a = order emp by hiredate ASC;
a = limit a 1; --top 1 optimisation
a= foreach a generate ename, hiredate;
dump a;
--(SMITH,1980-12-17T00:00:00.000+08:00)

--4. [E] (4 points): The number of employees in each department
a = join emp by deptno, dept by deptno;
a = foreach a generate .. $6,$8 ..;
b = foreach (group a by dname) generate group as dname, COUNT(a);
dump b;
--(SALES,6)
--(RESEARCH,5)
--(ACCOUNTING,3)

--5. [E] (4 points): The number of employees in each city
b = foreach (group a by loc) generate group as loc, COUNT(a);
dump b;
--(DALLAS,5)
--(CHICAGO,6)
--(NEW YORK,3)

--6. [E] (4 points): The average salary in each city
b = foreach (group a by loc) generate group as loc, SUM(a.sal)/COUNT(a) as avrSal;
dump b;
--(DALLAS,2175.0)
--(CHICAGO,1566.6666666666667)
--(NEW YORK,2916.6666666666665)

--7. [E] (4 points): The highest paid employee in each department
a1 = foreach a generate dname, ename, sal;
b = foreach (group a1 by dname) {
      sorted = order a1 by sal DESC;
      top = limit sorted 1;
      top = foreach top generate ename, sal;
      generate group as dept, flatten(top); 
    };
dump b;
--(ACCOUNTING,KING,5000.0)
--(RESEARCH,FORD,3000.0)
--(SALES,BLAKE,2850.0)

--8. [D] (4 points): Managers whose subordinates have at least one subordinate
worker = foreach emp generate empno, mgr;
boss = foreach emp generate empno, mgr;
name = foreach emp generate empno, ename;

boss_worker = join boss by empno left, worker by mgr;
boss_worker = filter boss_worker by (worker::empno is not null);

boss_worker_worker = foreach boss_worker generate boss::empno, worker::empno, worker::mgr;
boss_worker_worker = join boss_worker_worker by worker::empno left, worker by mgr;
boss_worker_worker = filter boss_worker_worker by ($3 is not null);

who = join name by empno, boss_worker_worker by $0;
who = foreach who generate name::ename;
who = distinct who;
dump who;
--(KING)
--(JONES)

--9. [D] (4 points): The number of employees for each hiring year
a = foreach emp generate empno, GetYear(hiredate) as YrOfHire; --GetYear is built-in UDF
b = foreach (group a by YrOfHire) generate group as YrOfHire, COUNT(a) as newHires;
dump b;
--(1980,1)
--(1981,10)
--(1987,2)
--(1991,1)

--10. [D] (4 points): The pay grade of each employee
a = foreach emp generate ename, sal;
b = cross a, salgrade;
b = filter b by ((sal>=losal) and (sal<=hisal));
b = foreach b generate ename,sal,grade;
dump b;
--(MILLER,1300.0,2)
--(FORD,3000.0,4)
--(JAMES,950.0,1)
--(ADAMS,1100.0,1)
--(TURNER,1500.0,3)
--(KING,5000.0,5)
--(SCOTT,3000.0,4)
--(CLARK,2450.0,4)
--(BLAKE,2850.0,4)
--(MARTIN,1250.0,2)
--(JONES,2975.0,4)
--(WARD,1250.0,2)
--(ALLEN,1600.0,3)
--(SMITH,800.0,1)


