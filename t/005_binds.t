# -*- perl -*-
use strict;
use warnings;
use Test::More tests => 13 * 2 + 1;

BEGIN { use_ok( 'DBIx::Array' ); }

my $connection={
                 "DBD::SQLite" => "dbi:SQLite:dbname=:memory",
                 "DBD::CSV"    => "dbi:CSV:f_dir=.",
                 "DBD::XBase"  => "dbi:XBase:.",
               };

foreach my $driver ("DBD::CSV", "DBD::XBase") { 
  #I can't get "DBD::SQLite" to pass tests on many platforms.
  my $dba=DBIx::Array->new;
  isa_ok($dba, 'DBIx::Array');

  SKIP: {
    eval "require $driver";
    skip "Database driver $driver not installed", 113 if $@;
    diag("Found database driver $driver");
  
    die("connection not defined for $driver") unless $connection->{$driver};
    $dba->connect($connection->{$driver}, "", "", {RaiseError=>0, AutoCommit=>1});
    my $table="dbixarray";
  
   #$dba->dbh->do("DROP TABLE $table");
    $dba->dbh->do("CREATE TABLE $table (F1 INTEGER,F2 CHAR(1),F3 VARCHAR(10))");
    is($dba->update("INSERT INTO $table (F1,F2,F3) VALUES (?,?,?)", 0,1,2), 1, 'insert');
    is($dba->update("INSERT INTO $table (F1,F2,F3) VALUES (?,?,?)", 1,2,3), 1, 'insert');
    is($dba->update("INSERT INTO $table (F1,F2,F3) VALUES (?,?,?)", 2,3,4), 1, 'insert');
    
    #diag("Single bind value");
    is($dba->sqlscalar("SELECT F1 FROM $table WHERE F3 = ?",        4 ), "2", 'Array Bind');
    is($dba->sqlscalar("SELECT F1 FROM $table WHERE F3 = ?",   (    4)), "2", 'Array Bind');
    is($dba->sqlscalar("SELECT F1 FROM $table WHERE F3 = ?",   [    4]), "2", 'aref Bind');
    SKIP: {
      skip "Database driver $driver does not support named parameters", 1 if $driver eq "DBD::CSV";
      is($dba->sqlscalar("SELECT F1 FROM $table WHERE F3 = :id", {id=>4}), "2", 'href Bind');
    }

    #diag("Two bind values");

    is($dba->sqlscalar("SELECT F1 FROM $table WHERE F3 = ? AND F2 = ?",        4, 3 ), "2", 'Array Bind');
    is($dba->sqlscalar("SELECT F1 FROM $table WHERE F3 = ? AND F2 = ?",   (    4, 3)), "2", 'Array Bind');
    is($dba->sqlscalar("SELECT F1 FROM $table WHERE F3 = ? AND F2 = ?",   [    4, 3]), "2", 'aref Bind');
    SKIP: {
      skip "Database driver $driver does not support named parameters", 1 if $driver eq "DBD::CSV";
      is($dba->sqlscalar("SELECT F1 FROM $table WHERE F3 = :id AND F2 = :f2", {id=>4, f2 =>3}), "2", 'href Bind');
    }

    #diag("Extra bind values does not cause error");

    SKIP: {
      skip "Database driver $driver does not support named parameters", 1 if $driver eq "DBD::CSV";
      is($dba->sqlscalar("SELECT F1 FROM $table WHERE F3 = :id AND F2 = :f2", {id=>4, f2 =>"3", x => "y"}), "2", 'href Bind');
    }
    
    $dba->dbh->do("DROP TABLE $table");
  }
}
