use ExtUtils::MakeMaker;
WriteMakefile(
    NAME         => 'DBIx-Array',
    VERSION_FROM => 'lib/DBIx/Array.pm',
    AUTHOR       => 'Michael R. Davis (domain=>stopllc,tld=>com,account=>mdavis)',
    ABSTRACT     => 'This modules is a DBI with array interfaces',
    PREREQ_PM    => {
                     'strict'      => 0,
                     'vars'        => 0,
                     'DBI'         => 0,
                     'Test::Simple'=> 0.44,
                     'Test::More'  => 0,
                     'DBD::CSV'    => 0,
                     'XML::Simple' => 0,
                     'Text::CSV_XS'=> 0,
                    },
);
