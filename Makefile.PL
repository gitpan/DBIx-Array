use ExtUtils::MakeMaker;
WriteMakefile(
    NAME         => 'DBIx-Array',
    VERSION_FROM => 'lib/DBIx/Array.pm',
    AUTHOR       => 'Michael R. Davis (domain=>stopllc,tld=>com,account=>mdavis)',
    ABSTRACT     => 'This modules is a DBI with array interfaces',
    PREREQ_PM    => {
                     'strict'         => 0,
                     'warnings'       => 0,
                     'DBI'            => 0,
                     'Test::Simple'   => 0.44,
                     'Test::More'     => 0,
                     'DBD::CSV'       => 0.21, #for "DROP TABLE IF EXISTS"
                     'XML::Simple'    => 0, #for exports
                     'Text::CSV_XS'   => 0,
                     'SQL::Statement' => 1.005,  #Tests
                     'IO::Scalar'     => 0, #for exports
                    },
);
