package DBIx::Array;
use strict;
use warnings;
use DBI;

our $VERSION='0.18';

=head1 NAME

DBIx::Array - This module is a wrapper around DBI with array interfaces

=head1 SYNOPSIS

  use DBIx::Array;
  my $dbx=DBIx::Array->new;
  $dbx->connect($connection, $user, $pass, \%opt); #passed to DBI
  my @array=$dbx->sqlarray($sql, @params);

=head1 DESCRIPTION

This module is for people who understand SQL and who understand fairly complex Perl data structures.  If you undstand how to modify your SQL to meet your data requirements then this module is for you.  In the example below, only one line of code is needed to generate an entire HTML table. 

  print &tablename($dba->sqlarrayarrayname(&sql, 15)), "\n";
   
  sub tablename {
    use CGI; my $html=CGI->new(""); #you would pass this reference
    return $html->table($html->Tr([map {$html->td($_)} @_]));
  }
   
  sub sql { #Oracle SQL
    return q{SELECT LEVEL AS "Number",
                    TRIM(TO_CHAR(LEVEL, 'rn')) as "Roman Numeral"
               FROM DUAL CONNECT BY LEVEL <= ? ORDER BY LEVEL};
  }

This module is used to connect to Oracle 10g (L<DBD::Oracle>), MySql 4 and 5 (L<DBD::mysql>) and Microsoft SQL Server (L<DBD::Sybase>) databases in a 24x7 production environment.  The test are written against L<DBD::SQLite>, L<DBD::CSV> and L<DBD::XBase>.

=head1 USAGE

=head1 CONSTRUCTOR

=head2 new

  my $dbx=DBIx::Array->new();
  $dbx->connect(...); #connect to database, sets and returns dbh

  my $dbx=DBIx::Array->new(dbh=>$dbh); #aready have a handle

=cut

sub new {
  my $this = shift();
  my $class = ref($this) || $this;
  my $self = {};
  bless $self, $class;
  $self->initialize(@_);
  return $self;
}

=head1 METHODS

=head2 initialize

=cut

sub initialize {
  my $self = shift();
  %$self=@_;
}

=head1 METHODS (Properties)

=head2 name

Set or returns a user friendly identification string for this database connection

  my $name=$dbx->name;
  my $name=$dbx->name($string);

=cut

sub name {
  my $self=shift;
  $self->{'name'}=shift if @_;
  return $self->{'name'};
}

=head1 METHODS (DBI Wrappers)

=head2 connect

Connects to the database and returns the database handle.

  $dbx->connect($connection, $user, $pass, \%opt);

Pass through to DBI->connect;

Examples: 

  $dbx->connect("DBI:mysql:database=mydb;host=myhost", "user", "pass", {AutoCommit=>1, RaiseError=>1});

  $dbx->connect("DBI:Sybase:server=myhost;datasbase=mydb", "user", "pass", {AutoCommit=>1, RaiseError=>1}); #Microsoft SQL Server API is same as Sybase API

  $dbx->connect("DBI:Oracle:TNSNAME", "user", "pass", {AutoCommit=>1, RaiseError=>1});

=cut

sub connect {
  my $self=shift();
  my $dbh=DBI->connect(@_);
  return $self->dbh($dbh);
}

=head2 disconnect

Calls $dbh->disconnect

  $dbx->disconnect;

Pass through to dbh->disconnect

=cut

sub disconnect {
  my $self=shift;
  return $self->dbh->disconnect
}

=head2 commit

Pass through to dbh->commit

  $dbx->commit;

=cut

sub commit {
  my $self=shift;
  return $self->dbh->commit;
}

=head2 rollback

Pass through to dbh->rollback

  $dbx->rollback;

=cut

sub rollback {
  my $self=shift;
  return $self->dbh->rollback;
}

=head2 AutoCommit

Pass through to  dbh->{'AutoCommit'} or dbh->{'AutoCommit'}=shift;

  $dbx->AutoCommit(1);
  &doSomething if $dbx->AutoCommit;

=cut

sub AutoCommit {
  my $self=shift;
  if (@_) {
    $self->dbh->{'AutoCommit'}=shift;
  }
  return $self->dbh->{'AutoCommit'};
}

=head2 RaiseError

Pass through to  dbh->{'RaiseError'} or dbh->{'RaiseError'}=shift;

  $dbx->RaiseError(1);
  &doSomething if $dbx->RaiseError;

=cut

sub RaiseError {
  my $self=shift;
  if (@_) {
    $self->dbh->{'RaiseError'}=shift;
  }
  return $self->dbh->{'RaiseError'};
}

=head2 errstr

Returns $DBI::errstr

  $dbx->errstr;

=cut

sub errstr {$DBI::errstr};

=head2 dbh

Sets or returns the database handle object.

  $dbx->dbh;
  $dbx->dbh($dbh);  #if you already have a connection

=cut

sub dbh {
  my $self = shift();
  $self->{'dbh'}=shift() if @_;
  return $self->{'dbh'};
}

=head1 METHODS (Read)

=head2 sqlcursor

Returns the prepared and executed SQL cursor so that you can use the cursor elsewhere.  Every method in this package uses this single method to generate a sqlcursor.

  my $sth=$dbx->sqlcursor($sql, \@param); #binds are ? values are positional
  my $sth=$dbx->sqlcursor($sql,  @param); #binds are ? values are positional
  my $sth=$dbx->sqlcursor($sql, \%param); #binds are :key

Note: In true Perl fashion extra hash binds are ignored.

  my @foo=$dbx->sqlarray("select :foo, :bar from dual",
                         {foo=>"a", bar=>1, baz=>"buz"}); #returns ("a", 1)

  my $one=$dbx->sqlscalar("select ? from dual", ["one"]); #returns "one"

  my $two=$dbx->sqlscalar("select ? from dual", "two");   #returns "two"

  my $inout=3;
  $dbx->execute("BEGIN :inout := :inout * 2; END;", {inout=>\$inout});
  print "$inout\n";  #$inout is 6

=cut

sub sqlcursor {
  my $self=shift;
  my $sql=shift;
  my $sth=$self->dbh->prepare($sql)    or die($self->errstr);
  if (ref($_[0]) eq "ARRAY") {
    $sth->execute(@{$_[0]})            or die($self->errstr);
  } elsif (ref($_[0]) eq "HASH") {
    foreach my $key (keys %{$_[0]}) {
      next unless $sql=~m/:$key\b/;
      if (ref($_[0]->{$key}) eq "SCALAR") {
        $sth->bind_param_inout(":$key" => $_[0]->{$key}, 255);
      } else {
        $sth->bind_param(":$key" => $_[0]->{$key});
      }
    } 
    $sth->execute                      or die($self->errstr);
  } else {
    $sth->execute(@_)                  or die($self->errstr);
  }
  return $sth;
}

=head2 sqlscalar

Returns the SQL query as a scalar.

This works great for selecting one value.

  $scalar=$dbx->sqlscalar($sql,  @parameters); #returns $
  $scalar=$dbx->sqlscalar($sql, \@parameters); #returns $
  $scalar=$dbx->sqlscalar($sql, \%parameters); #returns $

=cut

sub sqlscalar {
  my $self=shift();
  my @data=$self->sqlarray(@_);
  return $data[0];
}

=head2 sqlarray

Returns the SQL query as an array or array reference.

This works great for selecting one column from a table or selecting one row from a table.

  $array=$dbx->sqlarray($sql,  @parameters); #returns [$,$,$,...]
  @array=$dbx->sqlarray($sql,  @parameters); #returns ($,$,$,...)
  $array=$dbx->sqlarray($sql, \@parameters); #returns [$,$,$,...]
  @array=$dbx->sqlarray($sql, \@parameters); #returns ($,$,$,...)
  $array=$dbx->sqlarray($sql, \%parameters); #returns [$,$,$,...]
  @array=$dbx->sqlarray($sql, \%parameters); #returns ($,$,$,...)

=cut

sub sqlarray {
  my $self=shift();
  my $rows=$self->sqlarrayarray(@_);
  my @rows=map {@$_} @$rows;
  return wantarray ? @rows : \@rows;
}

=head2 sqlhash

Returns the first two columns of the SQL query as a hash or hash reference {Key=>Value, Key=>Value, ...}

  $hash=$dbx->sqlhash($sql,  @parameters); #returns {$=>$, $=>$, ...}
  %hash=$dbx->sqlhash($sql,  @parameters); #returns ($=>$, $=>$, ...)
  @hash=$dbx->sqlhash($sql,  @parameters); #this is ordered
  @keys=grep {!($n++ % 2)} @hash;         #ordered keys

  $hash=$dbx->sqlhash($sql, \@parameters); #returns {$=>$, $=>$, ...}
  %hash=$dbx->sqlhash($sql, \@parameters); #returns ($=>$, $=>$, ...)
  $hash=$dbx->sqlhash($sql, \%parameters); #returns {$=>$, $=>$, ...}
  %hash=$dbx->sqlhash($sql, \%parameters); #returns ($=>$, $=>$, ...)

=cut

sub sqlhash {
  my $self=shift();
  my $rows=$self->sqlarrayarray(@_);
  my @rows=map {$_->[0], $_->[1]} @$rows;
  return wantarray ? @rows : {@rows};
}

=head2 sqlarrayarray

Returns the SQL data as an array or array ref of array references ([],[],...) or [[],[],...]

  $array=$dbx->sqlarrayarray($sql,  @parameters); #returns [[$,$,...],[],[],...]
  @array=$dbx->sqlarrayarray($sql,  @parameters); #returns ([$,$,...],[],[],...)
  $array=$dbx->sqlarrayarray($sql, \@parameters); #returns [[$,$,...],[],[],...]
  @array=$dbx->sqlarrayarray($sql, \@parameters); #returns ([$,$,...],[],[],...)
  $array=$dbx->sqlarrayarray($sql, \%parameters); #returns [[$,$,...],[],[],...]
  @array=$dbx->sqlarrayarray($sql, \%parameters); #returns ([$,$,...],[],[],...)

=cut

sub sqlarrayarray {
  my $self=shift();
  my $sql=shift();
  return $self->_sqlarrayarray(sql=>$sql, param=>[@_], name=>0);
}

=head2 sqlarrayarrayname

Returns the SQL data as an array or array ref of array references ([],[],...) or [[],[],...] where the first rows is the column names

  $array=$dbx->sqlarrayarrayname($sql,  @parameters); #returns [[$,$,...],[]...]
  @array=$dbx->sqlarrayarrayname($sql,  @parameters); #returns ([$,$,...],[]...)
  $array=$dbx->sqlarrayarrayname($sql, \@parameters); #returns [[$,$,...],[]...]
  @array=$dbx->sqlarrayarrayname($sql, \@parameters); #returns ([$,$,...],[]...)
  $array=$dbx->sqlarrayarrayname($sql, \%parameters); #returns [[$,$,...],[]...]
  @array=$dbx->sqlarrayarrayname($sql, \%parameters); #returns ([$,$,...],[]...)

=cut

sub sqlarrayarrayname {
  my $self=shift();
  my $sql=shift();
  return $self->_sqlarrayarray(sql=>$sql, param=>[@_], name=>1);
}

=head2 _sqlarrayarray

  $array=$dbx->_sqlarrayarray(sql=>$sql, param=>[ @parameters], name=>1);
  @array=$dbx->_sqlarrayarray(sql=>$sql, param=>[ @parameters], name=>1);
  $array=$dbx->_sqlarrayarray(sql=>$sql, param=>[ @parameters], name=>0);
  @array=$dbx->_sqlarrayarray(sql=>$sql, param=>[ @parameters], name=>0);

  $array=$dbx->_sqlarrayarray(sql=>$sql, param=>[\@parameters], name=>1);
  @array=$dbx->_sqlarrayarray(sql=>$sql, param=>[\@parameters], name=>1);
  $array=$dbx->_sqlarrayarray(sql=>$sql, param=>[\@parameters], name=>0);
  @array=$dbx->_sqlarrayarray(sql=>$sql, param=>[\@parameters], name=>0);

  $array=$dbx->_sqlarrayarray(sql=>$sql, param=>[\%parameters], name=>1);
  @array=$dbx->_sqlarrayarray(sql=>$sql, param=>[\%parameters], name=>1);
  $array=$dbx->_sqlarrayarray(sql=>$sql, param=>[\%parameters], name=>0);
  @array=$dbx->_sqlarrayarray(sql=>$sql, param=>[\%parameters], name=>0);

=cut

sub _sqlarrayarray {
  my $self=shift();
  my %data=@_;
  my $sth=$self->sqlcursor($data{'sql'}, @{$data{'param'}}) or die($self->errstr);
  my $name=$sth->{'NAME'}; #DBD::mysql must store this first
  my $row=[];
  my @rows=();
  while ($row=$sth->fetchrow_arrayref()) {
    push @rows, [@$row];
  }
  unshift @rows, $name if $data{'name'};
  $sth->finish;
  return wantarray ? @rows : \@rows;
}

=head2 sqlarrayhash

Returns the SQL data as an array or array ref of hash references ({},{},...) or [{},{},...]

  $array=$dbx->sqlarrayhash($sql,  @parameters); #returns [{},{},{},...]
  @array=$dbx->sqlarrayhash($sql,  @parameters); #returns ({},{},{},...)
  $array=$dbx->sqlarrayhash($sql, \@parameters); #returns [{},{},{},...]
  @array=$dbx->sqlarrayhash($sql, \@parameters); #returns ({},{},{},...)
  $array=$dbx->sqlarrayhash($sql, \%parameters); #returns [{},{},{},...]
  @array=$dbx->sqlarrayhash($sql, \%parameters); #returns ({},{},{},...)

=cut

sub sqlarrayhash {
  my $self=shift();
  my $sql=shift();
  return $self->_sqlarrayhash(sql=>$sql, param=>[@_], name=>0);
}

=head2 sqlarrayhashname

Returns the SQL of data as an array or array ref of hash references ([],{},{},...) or [[],{},{},...] where the first rows is an array reference of the column names

  $array=$dbx->sqlarrayhashname($sql,  @parameters); #returns [[],{},{},...]
  @array=$dbx->sqlarrayhashname($sql,  @parameters); #returns ([],{},{},...)
  $array=$dbx->sqlarrayhashname($sql, \@parameters); #returns [[],{},{},...]
  @array=$dbx->sqlarrayhashname($sql, \@parameters); #returns ([],{},{},...)
  $array=$dbx->sqlarrayhashname($sql, \%parameters); #returns [[],{},{},...]
  @array=$dbx->sqlarrayhashname($sql, \%parameters); #returns ([],{},{},...)

=cut

sub sqlarrayhashname {
  my $self=shift();
  my $sql=shift();
  return $self->_sqlarrayhash(sql=>$sql, param=>[@_], name=>1);
}

=head2 _sqlarrayhash

Returns the SQL data as an array or array ref of hash references ({},{},...) or [{},{},...]

  $array=$dbx->_sqlarrayhash(sql=>$sql, param=>\@parameters, name=>1);
  @array=$dbx->_sqlarrayhash(sql=>$sql, param=>\@parameters, name=>1);
  $array=$dbx->_sqlarrayhash(sql=>$sql, param=>\@parameters, name=>0);
  @array=$dbx->_sqlarrayhash(sql=>$sql, param=>\@parameters, name=>0);

=cut

sub _sqlarrayhash {
  my $self=shift();
  my %data=@_;
  my $sth=$self->sqlcursor($data{'sql'}, @{$data{'param'}}) or die($self->errstr);
  my $row=[];
  my @rows=();
  while ($row=$sth->fetchrow_hashref()) {
    push @rows, {%$row};
  }
  unshift @rows, $sth->{'NAME'} if $data{'name'};
  $sth->finish;
  return wantarray ? @rows : \@rows;
}

=head2 sqlsort

Returns the SQL statments with the correct ORDER BY clause given a SQL statment (without an ORDER BY clause) and a signed 
integer on which column to sort.

  my $sql=$dbx->sqlsort(qq{SELECT 1,'Z' FROM DUAL UNION SELECT 2,'A' FROM DUAL}, -2);

Returns

  SELECT 1,'Z' FROM DUAL UNION SELECT 2,'A' FROM DUAL ORDER BY 2 DESC

=cut 

sub sqlsort {
  my $self=shift;
  my $sql=shift;
  my $sort=shift;
  if (defined($sort) and $sort=int($sort)) {
    my $column=abs($sort);
    my $direction = $sort < 0 ? "DESC" : "ASC";
    return join " ", $sql, sprintf("ORDER BY %u %s", $column, $direction);  
  } else {
    return $sql;
  }
}

=head2 sqlarrayarraynamesort

Returns a sqlarrayarrayname for $sql sorted on column $n where n is an integer asending for positive, desending for negative, 
and 0 for no sort.

  my $data=$dbx->sqlarrayarraynamesort($sql, $n,  @parameters);
  my $data=$dbx->sqlarrayarraynamesort($sql, $n, \@parameters);
  my $data=$dbx->sqlarrayarraynamesort($sql, $n, \%parameters);

Note: $sql must not have an "ORDER BY" clause in order for this function to work corectly.

=cut

sub sqlarrayarraynamesort {
  my $self=shift;
  my $sql=shift;
  my $sort=shift;
  return $self->sqlarrayarrayname($self->sqlsort($sql, $sort), @_);
} 

=head1 METHODS (Write)

=head2 update, delete, execute, insert

Returns the number of rows updated or deleted by the SQL statement.

  $rows=$dbx->update( $sql,  @parameters);
  $rows=$dbx->delete( $sql,  @parameters);
  $rows=$dbx->execute($sql, \@parameters);
  $rows=$dbx->execute($sql, \%parameters);

Remember to commit or use AutoCommit

Note: It appears that some drivers do not support the count of rows.  For example, DBD::Oracle does not support row counts on delete instead the value apears to be a success code.

Note: Currently update, insert, delete, and execute all point to the same method.  This may change in the future if we need to change the behavior of one method.  So, please use the correct method name for your function.

=cut

*insert=\&update;
*delete=\&update;
*exec=\&update;   #deprecated
*execute=\&update;

sub update {
  my $self=shift();
  my $sql=shift();
  my $sth=$self->sqlcursor($sql, @_) or die($self->errstr);
  my $rows=$sth->rows;
  $sth->finish;
  return $rows;
}

=head1 TODO

I would like to add caching service in the sqlcursor method.

=head1 BUGS

=head1 SUPPORT

DavisNetworks.com supports all Perl applications big or small.

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

L<DBI>, L<DBIx::DWIW>, L<DBIx::Wrapper>, L<DBIx::Simple>

=cut

1;
