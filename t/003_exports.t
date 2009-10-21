# -*- perl -*-

# t/001_load.t - check module loading and create testing directory

use Test::More tests => 9;

BEGIN { use_ok( 'DBIx::Array' ); }

my $dba = DBIx::Array->new ();
isa_ok ($dba, 'DBIx::Array');
$dba->connect("dbi:CSV:f_dir=.", "", "", {RaiseError=>1, AutoCommit=>1});
my $table="DBIxArray";
$dba->dbh->do("DROP TABLE IF EXISTS $table");
$dba->dbh->do("CREATE TABLE $table (Col1 INTEGER,Col2 CHAR(1),Col3 VARCHAR(20))");
is($dba->update("INSERT INTO $table (Col1,Col2,Col3)
                 VALUES (?,?,?)", 1,"a",q{Say "Yes!"}), 1, 'insert');
is($dba->update("INSERT INTO $table (Col1,Col2,Col3)
                 VALUES (?,?,?)", 2,"b",q{One, Two, or Three}), 1, 'insert');
is($dba->update("INSERT INTO $table (Col1,Col2,Col3)
                 VALUES (?,?,?)", 3,"c",q{OK, "Already"}), 1, 'insert');
my $data=$dba->sqlarrayarrayname("SELECT * FROM $table ORDER BY Col1");
my $csv=q{Col1,Col2,Col3
1,a,"Say ""Yes!"""
2,b,"One, Two, or Three"
3,c,"OK, ""Already"""
};

is($dba->csv_arrayarrayname($data), $csv, 'csv_arrayarrayname');

my $xml=q{<?xml version='1.0' standalone='yes'?>
<document>
  <body>
    <rows>
      <row>
        <Col1>1</Col1>
        <Col2>a</Col2>
        <Col3>Say &quot;Yes!&quot;</Col3>
      </row>
      <row>
        <Col1>2</Col1>
        <Col2>b</Col2>
        <Col3>One, Two, or Three</Col3>
      </row>
      <row>
        <Col1>3</Col1>
        <Col2>c</Col2>
        <Col3>OK, &quot;Already&quot;</Col3>
      </row>
    </rows>
  </body>
  <head>
    <columns>
      <column uom="unit1">Col1</column>
      <column>Col2</column>
      <column uom="unit2">Col3</column>
    </columns>
    <counts>
      <columns>3</columns>
      <rows>3</rows>
    </counts>
  </head>
</document>
};
$data=$dba->sqlarrayhashname("SELECT * FROM $table ORDER BY Col1");
is($dba->xml_arrayhashname(data=>$data,
                           uom=>{Col1=>"unit1", Col3=>"unit2"}), $xml, "xml_arrayhashname");

my $sth=$dba->sqlcursor("SELECT * FROM $table ORDER BY Col1");

isa_ok($sth, "DBI::st");
use IO::Scalar;
$data='';
my $fh=IO::Scalar->new(\$data);
$dba->csv_cursor($fh, $sth);
is($data, $csv, "csv_cursor");

$dba->dbh->do("DROP TABLE IF EXISTS $table");
