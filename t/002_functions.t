# -*- perl -*-

# t/001_load.t - check module loading and create testing directory

use Test::More tests => 115;

BEGIN { use_ok( 'DBIx::Array' ); }

my $dba = DBIx::Array->new ();
isa_ok ($dba, 'DBIx::Array');
$dba->connect("dbi:CSV:f_dir=.", "", "", {RaiseError=>1, AutoCommit=>1});
my $table="DBIxArray";
#open TABLE, $table;
#print TABLE, "f1,f2,f3\n1,a,A\n2,b,B\n3,c,C\n";
#close TABLE;

$dba->dbh->do("DROP TABLE IF EXISTS $table");
$dba->dbh->do("CREATE TABLE $table (f1 INTEGER,f2 CHAR(1),f3 VARCHAR(10))");
is($dba->update("INSERT INTO $table (f1,f2,f3) VALUES (?,?,?)", 0,1,2), 1, 'insert');
is($dba->update("INSERT INTO $table (f1,f2,f3) VALUES (?,?,?)", 1,2,3), 1, 'insert');
is($dba->update("INSERT INTO $table (f1,f2,f3) VALUES (?,?,?)", 2,3,4), 1, 'insert');

isa_ok($dba->sqlcursor("SELECT * FROM $table"), 'DBI::st', 'sqlcursor');

my $array=$dba->sqlarray("SELECT f1,f2,f3 FROM $table WHERE f1 = ?", 0);
isa_ok($array, "ARRAY", '$dba->sqlarray scalar context');
is(scalar(@$array), 3, 'scalar(@$array)');
is($array->[0], 0, '$dba->sqlarray->[0]');
is($array->[1], 1, '$dba->sqlarray->[1]');
is($array->[2], 2, '$dba->sqlarray->[2]');

my @array=$dba->sqlarray("SELECT f1,f2,f3 FROM $table WHERE f1 = ?", 0);
is(scalar(@array), 3, 'scalar(@$array)');
is($array[0], 0, '$dba->sqlarray[0]');
is($array[1], 1, '$dba->sqlarray[1]');
is($array[2], 2, '$dba->sqlarray[2]');

my $hash=$dba->sqlhash("SELECT f1,f2 FROM $table");
isa_ok($hash, "HASH", 'sqlarray scalar context');
is($hash->{'0'}, 1, 'sqlhash');
is($hash->{'1'}, 2, 'sqlhash');
is($hash->{'2'}, 3, 'sqlhash');

my %hash=$dba->sqlhash("SELECT f1,f2 FROM $table");
is($hash{'0'}, 1, 'sqlhash');
is($hash{'1'}, 2, 'sqlhash');
is($hash{'2'}, 3, 'sqlhash');

$array=$dba->sqlarrayarray("SELECT f1,f2,f3 FROM $table ORDER BY f1");
isa_ok($array, "ARRAY", 'sqlarrayarray scalar context');
isa_ok($array->[0], "ARRAY", 'sqlarrayarray row 1');
isa_ok($array->[1], "ARRAY", 'sqlarrayarray row 2');
isa_ok($array->[2], "ARRAY", 'sqlarrayarray row 3');
is($array->[0]->[0], 0, 'data');
is($array->[0]->[1], 1, 'data');
is($array->[0]->[2], 2, 'data');
is($array->[1]->[0], 1, 'data');
is($array->[1]->[1], 2, 'data');
is($array->[1]->[2], 3, 'data');
is($array->[2]->[0], 2, 'data');
is($array->[2]->[1], 3, 'data');
is($array->[2]->[2], 4, 'data');

@array=$dba->sqlarrayarray("SELECT f1,f2,f3 FROM $table ORDER BY f1");
isa_ok($array[0], "ARRAY", 'sqlarrayarray row 1');
isa_ok($array[1], "ARRAY", 'sqlarrayarray row 2');
isa_ok($array[2], "ARRAY", 'sqlarrayarray row 3');
is($array[0]->[0], 0, 'data');
is($array[0]->[1], 1, 'data');
is($array[0]->[2], 2, 'data');
is($array[1]->[0], 1, 'data');
is($array[1]->[1], 2, 'data');
is($array[1]->[2], 3, 'data');
is($array[2]->[0], 2, 'data');
is($array[2]->[1], 3, 'data');
is($array[2]->[2], 4, 'data');

$array=$dba->sqlarrayarrayname("SELECT f1,f2,f3 FROM $table ORDER BY f1");
isa_ok($array, "ARRAY", 'sqlarrayarrayname scalar context');
isa_ok($array->[0], "ARRAY", 'sqlarrayarrayname header');
isa_ok($array->[1], "ARRAY", 'sqlarrayarrayname row 1');
isa_ok($array->[2], "ARRAY", 'sqlarrayarrayname row 2');
isa_ok($array->[3], "ARRAY", 'sqlarrayarrayname row 3');
is($array->[0]->[0], 'f1', 'data');
is($array->[0]->[1], 'f2', 'data');
is($array->[0]->[2], 'f3', 'data');
is($array->[1]->[0], 0, 'data');
is($array->[1]->[1], 1, 'data');
is($array->[1]->[2], 2, 'data');
is($array->[2]->[0], 1, 'data');
is($array->[2]->[1], 2, 'data');
is($array->[2]->[2], 3, 'data');
is($array->[3]->[0], 2, 'data');
is($array->[3]->[1], 3, 'data');
is($array->[3]->[2], 4, 'data');

@array=$dba->sqlarrayarrayname("SELECT f1,f2,f3 FROM $table ORDER BY f1");
isa_ok($array[0], "ARRAY", 'sqlarrayarrayname header');
isa_ok($array[1], "ARRAY", 'sqlarrayarrayname row 1');
isa_ok($array[2], "ARRAY", 'sqlarrayarrayname row 2');
isa_ok($array[3], "ARRAY", 'sqlarrayarrayname row 3');
is($array[0]->[0], 'f1', 'data');
is($array[0]->[1], 'f2', 'data');
is($array[0]->[2], 'f3', 'data');
is($array[1]->[0], 0, 'data');
is($array[1]->[1], 1, 'data');
is($array[1]->[2], 2, 'data');
is($array[2]->[0], 1, 'data');
is($array[2]->[1], 2, 'data');
is($array[2]->[2], 3, 'data');
is($array[3]->[0], 2, 'data');
is($array[3]->[1], 3, 'data');
is($array[3]->[2], 4, 'data');

$array=$dba->sqlarrayhashname("SELECT f1,f2,f3 FROM $table ORDER BY f1");
isa_ok($array, "ARRAY", 'sqlarrayhashname scalar context');
isa_ok($array->[0], "ARRAY", 'sqlarrayhashname header');
isa_ok($array->[1], "HASH", 'sqlarrayhashname row 1');
isa_ok($array->[2], "HASH", 'sqlarrayhashname row 2');
isa_ok($array->[3], "HASH", 'sqlarrayhashname row 3');
is($array->[0]->[0], 'f1', 'data');
is($array->[0]->[1], 'f2', 'data');
is($array->[0]->[2], 'f3', 'data');
is($array->[1]->{'f1'}, 0, 'data');
is($array->[1]->{'f2'}, 1, 'data');
is($array->[1]->{'f3'}, 2, 'data');
is($array->[2]->{'f1'}, 1, 'data');
is($array->[2]->{'f2'}, 2, 'data');
is($array->[2]->{'f3'}, 3, 'data');
is($array->[3]->{'f1'}, 2, 'data');
is($array->[3]->{'f2'}, 3, 'data');
is($array->[3]->{'f3'}, 4, 'data');

@array=$dba->sqlarrayhashname("SELECT f1,f2,f3 FROM $table ORDER BY f1");
isa_ok($array[0], "ARRAY", 'sqlarrayhashname header');
isa_ok($array[1], "HASH", 'sqlarrayhashname row 1');
isa_ok($array[2], "HASH", 'sqlarrayhashname row 2');
isa_ok($array[3], "HASH", 'sqlarrayhashname row 3');
is($array[0]->[0], 'f1', 'data');
is($array[0]->[1], 'f2', 'data');
is($array[0]->[2], 'f3', 'data');
is($array[1]->{'f1'}, 0, 'data');
is($array[1]->{'f2'}, 1, 'data');
is($array[1]->{'f3'}, 2, 'data');
is($array[2]->{'f1'}, 1, 'data');
is($array[2]->{'f2'}, 2, 'data');
is($array[2]->{'f3'}, 3, 'data');
is($array[3]->{'f1'}, 2, 'data');
is($array[3]->{'f2'}, 3, 'data');
is($array[3]->{'f3'}, 4, 'data');

my $sql="SELECT f1,f2,f3 FROM $table";
is($dba->sqlsort($sql,1), "$sql ORDER BY 1 ASC", 'sqlsort');
is($dba->sqlsort($sql,-1), "$sql ORDER BY 1 DESC", 'sqlsort');

#This works great in DBD::Oracle but cant get DBD::CSV to play with others
#$sql="SELECT f1,f2,f3 FROM $table WHERE f2 >= 2";
#$array=$dba->sqlarrayarraynamesort($sql, -2);
#isa_ok($array, "ARRAY", 'sqlarrayarray scalar context');
#isa_ok($array->[0], "ARRAY", 'sqlarrayarrayname row 1');
#isa_ok($array->[1], "ARRAY", 'sqlarrayarrayname row 2');
#isa_ok($array->[2], "ARRAY", 'sqlarrayarrayname row 3');
#isa_ok($array->[3], "ARRAY", 'sqlarrayarrayname row 3');
#is($array->[0]->[0], 'f1', 'data');
#is($array->[0]->[1], 'f2', 'data');
#is($array->[0]->[2], 'f3', 'data');
#is($array->[1]->[0], 2, 'data');
#is($array->[1]->[1], 3, 'data');
#is($array->[1]->[2], 4, 'data');
#is($array->[2]->[0], 1, 'data');
#is($array->[2]->[1], 2, 'data');
#is($array->[2]->[2], 3, 'data');

#$dba->commit;
$dba->dbh->do("DROP TABLE IF EXISTS $table");
