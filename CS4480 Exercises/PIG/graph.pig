A = load 'ex_data/roadnet/roadNet-CA.txt' as (nodeA:chararray, nodeB:chararray);
B = group A by nodeA;
C = foreach B generate group as node, COUNT(A) as outgoing_freq;
illustrate C;
--dump C

--[E] (10%): the frequency of each degree value
D = foreach (group C by outgoing_freq) generate group as degree, COUNT(C) as degree_freq;
illustrate D;
--dump D;
--(1,30)
--(2,18)
--(3,153)
--(4,111)
--(5,5)
--(6,1)

--[E] (10%): the percentage of dead-end nodes
start = foreach A generate nodeA;
end = foreach A generate nodeB;
tgt = join end by $0 left, start by $0;
deadend = filter tgt by start::nodeA is null;
deadend = foreach deadend generate $0;
deadend = distinct deadend;
allnode = union start, end;
allnode = distinct allnode;
p1 = foreach deadend generate $0, 1;
p2 = foreach allnode generate $0, 2;
p = union p1,p2;
p = foreach (group p by $1) generate group, COUNT(p);
dump p;
--(1,141)
--(2,459)
--23.5%

--[E] (10%): the average degree of the graph
E = foreach D generate $0,$1,$0*$1 as tl, 1 as dummyvar;
F = foreach (group E by dummyvar) generate SUM(E.degree_freq) as freq, SUM(E.tl) as total;
F = foreach F generate $1/(float)$0 as average;
dump F;
--(3.144654)


--Triangle Counting
--[E] (10 points): Determine the size of the triangle relation

--a1 = foreach A generate $0 as n1:int,$1 as n2:int,
    --$0 as n3:int,$1 as n4:int,$0 as n5:int,$1 as n6:int;
--a2 = filter a1 by (n2==n3 and n4==n5 and n1==n6);

A1 = load 'ex_data/roadnet/roadNet-CA.txt' as (n1:chararray, n2:chararray);
A2 = load 'ex_data/roadnet/roadNet-CA.txt' as (n3:chararray, n4:chararray);
A3 = load 'ex_data/roadnet/roadNet-CA.txt' as (n5:chararray, n6:chararray);

AA = join A1 by n2, A2 by n3;
AA = foreach AA generate n1, n2, n4;
AAA = join AA by n4, A3 by n5;
AAA = filter AAA by (n6==n1);

ans = foreach AAA generate n1, n2, n5, '-' as dummyvar:chararray;
ans1 = foreach (group ans by dummyvar) generate COUNT(ans);
--(195)

--[D] (10 pints): Can you identify one problem with this approach here?
--have to keep 3 copies of roadNet-CA.txt dataset for joining
--to avoid pointers referencing same alias problem
