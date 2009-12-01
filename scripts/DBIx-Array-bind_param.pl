#!/usr/bin/perl

=head1 NAME

DBIx-Array-bind_param.pl - DBIx::Array Bind Examples

=head1 LIMITATIONS

Oracle SQL Syntax

=cut

use strict;
use warnings;
use DBIx::Array;
use Data::Dumper;

my $connect=shift or die; #written for DBD::Oracle
my $user=shift or die;
my $pass=shift or die;

my $dba=DBIx::Array->new;
$dba->connect($connect, $user, $pass, {AutoCommit=>1, RaiseError=>1});

my $sql=q{Select InitCap(:foo) AS "Foo" from dual};

my $data=$dba->sqlarrayarrayname($sql, {bar=>1, foo=>"foO", baz=>1});

print Dumper($data);

$data=$dba->sqlarrayarrayname(qq{SELECT 'A' AS "AAA" FROM DUAL});

print Dumper($data);

$data=$dba->sqlarrayarrayname(qq{SELECT ? AS "BBB" FROM DUAL}, ["B"]);

print Dumper($data);

$data=$dba->sqlarrayarrayname(qq{SELECT ? AS "CCC" FROM DUAL}, "C");

print Dumper($data);

my $bar=3;
print "In: $bar\n";
$dba->update("BEGIN :bar := :bar * 2; END;", {bar=>\$bar, foo=>1});
print "Out: $bar\n";

$data=$dba->sqlarrayarrayname(q{select :foo AS "Foo", :bar AS "Bar" from dual},
                              {foo=>"a", bar=>1, baz=>"buz"});

print Dumper($data);
