package DBIx::Array::Export;
use base qw{DBIx::Array};
use strict;
use warnings;

our $VERSION='0.17';
our $PACKAGE=__PACKAGE__;

=head1 NAME

DBIx::Array::Export - This modules extends DBIx::Array with convenient export functions

=head1 SYNOPSIS

  use DBIx::Array::Export;
  my $dbx=DBIx::Array::Export->new;
  $dbx->connect($connection, $user, $pass, \%opt); #passed to DBI

=head1 DESCRIPTION

=head1 USAGE

=head1 METHODS (Export)

=head2 xml_arrayhashname

Returns XML given an arrayhashname data structure
 
  $dbx->execute(q{ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS"Z"'});
  my @arrayhashname=$dbx->sqlarrayhashname($sql);
  my $xml=$dbx->xml_arrayhashname(data    => \@arrayhashname,
                                  comment => "Text String Comment",
                                  uom     => {col1=>"min", col2=>"ft"});

=cut

sub xml_arrayhashname {
  my $self=shift;
  my $opt={@_};
  my $data=$opt->{'data'} || [];
  $data=[] unless ref($data) eq "ARRAY";
  my $uom=$opt->{'uom'} || {};
  $uom={} unless ref($uom) eq "HASH";

  my $header=shift(@$data);
  foreach (@$data) {
    foreach my $key (keys %$_) {
      if (defined($_->{$key})) {
        $_->{$key}=[$_->{$key}];  #This is needed for XML::Simple to make pretty XML.
      } else {
        delete($_->{$key});     #This is a choice that I made but I'm not sure if it's smart
      }
    }
  }
  @$header=map {exists($uom->{$_})? {content=>$_, uom=>$uom->{$_}} : $_} @$header;

  my $module="XML::Simple";
  eval("use $module;");
  if ($@) {
    die("Error: $PACKAGE->xml_arrayhashname method requres $module");
  } else {
    my $xs=XML::Simple->new(XMLDecl=>1, RootName=>q{document}, ForceArray=>1);
    my $head={};
    $head->{'comment'}=[$opt->{'comment'}] if $opt->{'comment'};
    $head->{'columns'}=[{column=>$header}];
    $head->{'counts'}=[{rows=>[scalar(@$data)], columns=>[scalar(@$header)]}];
    return $xs->XMLout({
                         head=>$head,
                         body=>{rows=>[{row=>$data}]},
                       });
  }
}

=head2 csv_arrayarrayname

Returns CSV given an arrayarrayname data structure

  my $csv=$dbx->csv_arrayarrayname($data);

=cut

sub csv_arrayarrayname {
  my $self=shift;
  my $data=shift;
  my $module="Text::CSV_XS";
  eval("use $module;");
  if ($@) {
    die("Error: $PACKAGE->csv_arrayarrayname method requres $module");
  } else {
    my $csv=Text::CSV_XS->new;
    return join "", map {&join_csv($csv, @$_)} @$data;
  }

  sub join_csv {
    my $csv=shift;
    my $status=$csv->combine(@_);
    return $status ? $csv->string."\r\n" : undef; #\r\n per RFC 4180
  }
}

=head2 csv_cursor

Writes CSV to file handle given an executed cursor

  binmode($fh);
  $dbx->csv_cursor($fh, $sth);

Due to portablilty issues, I choose not to force the passed file handle into binmode.  However, it IS required!  For most file handle objects you can run binmode($fh) or $fh->binmode;

=cut

sub csv_cursor {
  my $self=shift;
  my $fh=shift;
  my $sth=shift;
  my $module="Text::CSV_XS";
  eval("use $module;");
  if ($@) {
    die("Error: $PACKAGE->csv_arrayarrayname method requres $module");
  } else {
    my $csv=Text::CSV_XS->new;
    $csv->print($fh, scalar($sth->{'NAME'}));
    print $fh "\r\n";
    my $row=[];
    while ($row=$sth->fetchrow_arrayref()) {
      $csv->print($fh, $row);
      print $fh "\r\n";
    }
    $sth->finish;
  }
}

=head2 xls_arrayarrayname

Returns XLS data blob given an arrayarrayname data structure

  my $xls=$dbx->xls_arrayarrayname("Tab One"=>$data, "Tab Two"=>$data2, ...);

=cut

sub xls_arrayarrayname {
  my $self=shift;
  my $module="Spreadsheet::WriteExcel::Simple::Tabs";
  eval("use $module;");
  if ($@) {
    die("Error: $PACKAGE->xls_arrayarrayname method requres $module");
  } else {
    my $ss=Spreadsheet::WriteExcel::Simple::Tabs->new();
    $ss->add(@_);
    return $ss->content;
  }
}

=head1 TODO

Switch out L<XML::Simple> for L<XML::LibXML::LazyBuilder>

Add XLS export with L<Spreadsheet::WriteExcel::Simple::Tabs>

=head1 BUGS

=head1 SUPPORT

=head1 AUTHOR

  Michael R. Davis
  CPAN ID: MRDVT
  STOP, LLC
  domain=>stopllc,tld=>com,account=>mdavis
  http://www.stopllc.com/

=head1 COPYRIGHT

This program is free software licensed under the...

  The BSD License

The full text of the license can be found in the
LICENSE file included with this module.

=head1 SEE ALSO

L<XML::Simple>, L<Text::CSV_XS>, L<Spreadsheet::WriteExcel::Simple::Tabs>

=cut

1;
