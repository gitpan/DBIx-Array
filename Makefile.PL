use ExtUtils::MakeMaker;
WriteMakefile(
 NAME         => 'DBIx-Array',
 VERSION_FROM => 'lib/DBIx/Array.pm',
 AUTHOR       => 'Michael R. Davis (domain=>stopllc,tld=>com,account=>mdavis)',
 ABSTRACT_FROM=> 'lib/DBIx/Array.pm',
 PREREQ_PM    => {
               'strict'                  => 0,
               'warnings'                => 0,
               'base'                    => 0,
               'DBI'                     => 0,
               'Test::Simple'            => 0,    #for Test::More
                 },
);
