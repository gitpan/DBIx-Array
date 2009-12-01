#!/usr/bin/perl

=head1 NAME

DBIx-Array-tablename.pl - DBIx::Array HTML Table Example

=head1 LIMITATIONS

Oracle SQL Syntax

=cut

use strict;
use warnings;
use DBIx::Array;

my $connect=shift or die; #written for DBD::Oracle
my $user=shift or die;
my $pass=shift or die;

my $dba=DBIx::Array->new;
$dba->connect($connect, $user, $pass, {AutoCommit=>1, RaiseError=>1});

my $sql=q{SELECT LEVEL AS "Number",
                 TRIM(TO_CHAR(LEVEL, 'rn')) as "Roman Numeral"
            FROM DUAL
      CONNECT BY LEVEL <= ?
        ORDER BY LEVEL};
my @data=$dba->sqlarrayarrayname($sql, 15); #[[Number=>"Roman Numeral"],
                                            # [1=>"i"], [2=>"ii"], ...]

print tablename(@data), "\n";

sub tablename {
  use CGI; my $html=CGI->new(""); #you would pass this reference
  return $html->table($html->Tr([map {$html->td($_)} @_]));
} 

