use ExtUtils::MakeMaker;
WriteMakefile(
 NAME         => 'DBIx-Array',
 VERSION_FROM => 'lib/DBIx/Array.pm',
 AUTHOR       => 'Michael R. Davis (domain=>stopllc,tld=>com,account=>mdavis)',
 ABSTRACT     => 'This module is a wrapper around DBI with array interfaces',
 PREREQ_PM    => {
               'strict'                  => 0,
               'warnings'                => 0,
               'DBI'                     => 0,
               'Test::More'              => 0,    #for Tests
               'SQL::Statement'          => 1.005,#for tests
               'DBD::CSV'                => 0.21, #for tests "DROP IF EXISTS"
                 },
);
