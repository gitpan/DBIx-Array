package DBIx::Array;
use strict;
use warnings;
use base qw{Package::New};
use Data::Dumper qw{Dumper};
use List::Util qw(sum);
use DBI;
use DBIx::Array::Session::Action;

our $VERSION='0.49';
our $PACKAGE=__PACKAGE__;

=head1 NAME

DBIx::Array - This module is a wrapper around DBI with array interfaces

=head1 SYNOPSIS

  use DBIx::Array;
  my $dbx=DBIx::Array->new;
  $dbx->connect($connection, $user, $pass, \%opt); #passed to DBI
  my @array=$dbx->sqlarray($sql, @params);

With a connected database handle

  use DBIx::Array;
  my $dbx=DBIx::Array->new(dbh=>$dbh);

With stored connection information from a File

  use DBIx::Array::Connect;
  my $dbx=DBIx::Array::Connect->new(file=>"my.ini")->connect("mydatabase");

=head1 DESCRIPTION

This module provides a Perl data structure interface for Structured Query Language (SQL).  This module is for people who truly understand SQL and who understand Perl data structures.  If you understand how to modify your SQL to meet your data requirements then this module is for you.

This module is used to connect to Oracle 10g and 11g using L<DBD::Oracle> on both Linux and Win32, MySQL 4 and 5 using L<DBD::mysql> on Linux, Microsoft SQL Server using L<DBD::Sybase> on Linux and using L<DBD::ODBC> on Win32 systems, and PostgreSQL using L<DBD::Pg> in a 24x7 production environment.  Tests are written against L<DBD::CSV> and L<DBD::XBase>.

=head1 USAGE

Loop through data

  foreach my $row ($dbx->sqlarrayhash($sql, @bind)) {
    do_something($row->{"id"}, $row->{"column"});
  }

Easily generate an HTML table

  my $cgi  = CGI->new("");
  my $html = $cgi->table($cgi->Tr([map {$cgi->td($_)} $dbx->sqlarrayarrayname($sql, @param)]));

Bless directly into a class

  my ($object) = $dbx->sqlarrayobject("My::Package", $sql, {id=>$id}); #bless({id=>1, name=>'foo'}, 'My::Package');
  my @objects  = $dbx->absarrayobject("My::Package", "myview", '*', {active=>1}, ["name"]); #($object, $object, ...)

=head1 CONSTRUCTOR

=head2 new

  my $dbx=DBIx::Array->new();
  $dbx->connect(...); #connect to database, sets and returns dbh

  my $dbx=DBIx::Array->new(dbh=>$dbh); #already have a handle

=cut

#See Package::New->new

=head1 METHODS (Properties)

=head2 dbh

Sets or returns the database handle object.

  my $dbh=$dbx->dbh;
  $dbx->dbh($dbh);  #if you already have a connection

=cut

sub dbh {
  my $self=shift;
  if (@_) {
    $self->{'dbh'}=shift;
    CORE::delete $self->{'_prepared'}; #clear cache if we switch handles
  }
  return $self->{'dbh'};
}

=head2 name

Sets or returns a user friendly identification string for this database connection

  my $name=$dbx->name;
  $dbx->name($string);

=cut

sub name {
  my $self=shift;
  $self->{'name'}=shift if @_;
  return $self->{'name'};
}

=head1 METHODS (DBI Wrappers)

=head2 connect

Wrapper around DBI->connect; Connects to the database, sets dbh property, and returns the database handle.

  $dbx->connect($connection, $user, $pass, \%opt); #sets $dbx->dbh
  my $dbh=$dbx->connect($connection, $user, $pass, \%opt);

Examples:

  $dbx->connect("DBI:mysql:database=mydb;host=myhost", "user", "pass", {AutoCommit=>1, RaiseError=>1});
  $dbx->connect("DBI:Sybase:server=myhost;datasbase=mydb", "user", "pass", {AutoCommit=>1, RaiseError=>1}); #Microsoft SQL Server API is same as Sybase API
  $dbx->connect("DBI:Oracle:TNSNAME", "user", "pass", {AutoCommit=>1, RaiseError=>1});

=cut

sub connect {
  my $self=shift;
  my $dbh=DBI->connect(@_);
  $self->dbh($dbh);
  CORE::delete $self->{'action'} if exists $self->{'action'};
  tie $self->{'action'}, "DBIx::Array::Session::Action", (parent=>$self);
  return $self->dbh;
}

=head2 disconnect

Wrapper around dbh->disconnect

  $dbx->disconnect;

=cut

sub disconnect {
  my $self=shift;
  untie $self->{'action'};
  CORE::delete $self->{'action'};
  return $self->dbh->disconnect
}

=head2 commit

Wrapper around dbh->commit

  $dbx->commit;

=cut

sub commit {
  my $self=shift;
  return $self->dbh->commit;
}

=head2 rollback

Wrapper around dbh->rollback

  $dbx->rollback;

=cut

sub rollback {
  my $self=shift;
  return $self->dbh->rollback;
}

=head2 prepare

Wrapper around dbh->prepare with a local cache.

  my $sth=$dbh->prepare($sql);

=cut

sub prepare {
  my $self  = shift;
  my $sql   = shift;
  my $cache = $self->{'_prepared'} ||= {};
  my $sth   = $cache->{$sql}       ||= $self->dbh->prepare($sql) or die($self->errstr);
  return $sth;
}

=head2 AutoCommit

Wrapper around dbh->{'AutoCommit'}

  $dbx->AutoCommit(1);
  &doSomething if $dbx->AutoCommit;

For transactions that must complete together, I recommend

  { #block to keep local... well... local.
    local $dbx->dbh->{'AutoCommit'}=0;
    $dbx->sqlinsert($sql1, @bind1);
    $dbx->sqlupdate($sql2, @bind2);
    $dbx->sqlinsert($sql3, @bind3);
  } #What is AutoCommit now?  Do you care?

If AutoCommit reverts to true at the end of the block then DBI commits.  Else AutoCommit is still false and still not committed.  This allows higher layers to determine commit functionality.

=cut

sub AutoCommit {
  my $self=shift;
  if (@_) {
    $self->dbh->{'AutoCommit'}=shift;
  }
  return $self->dbh->{'AutoCommit'};
}

=head2 RaiseError

Wrapper around dbh->{'RaiseError'}

  $dbx->RaiseError(1);
  &doSomething if $dbx->RaiseError;

  { #local block
    local $dbx->dbh->{'RaiseError'}=0;
    $dbx->sqlinsert($sql, @bind); #do not die
  }

=cut

sub RaiseError {
  my $self=shift;
  if (@_) {
    $self->dbh->{'RaiseError'}=shift;
  }
  return $self->dbh->{'RaiseError'};
}

=head2 errstr

Wrapper around $DBI::errstr

  my $err=$dbx->errstr;

=cut

sub errstr {$DBI::errstr};

=head1 METHODS (Read) - SQL

=head2 sqlcursor

Returns the prepared and executed SQL cursor so that you can use the cursor elsewhere.  Every method in this package uses this single method to generate a sqlcursor.

  my $sth=$dbx->sqlcursor($sql,  @param); #binds are ? values are positional
  my $sth=$dbx->sqlcursor($sql, \@param); #binds are ? values are positional
  my $sth=$dbx->sqlcursor($sql, \%param); #binds are :key

Note: In true Perl fashion extra hash binds are ignored.

  my @foo=$dbx->sqlarray("select :foo, :bar from dual",
                         {foo=>"a", bar=>1, baz=>"buz"}); #returns ("a", 1)

  my $one=$dbx->sqlscalar("select ? from dual", ["one"]); #returns "one"

  my $two=$dbx->sqlscalar("select ? from dual", "two");   #returns "two"

Scalar references are passed in and out with a hash bind.

  my $inout=3;
  $dbx->execute("BEGIN :inout := :inout * 2; END;", {inout=>\$inout});
  print "$inout\n";  #$inout is 6

Direct Plug-in for L<SQL::Abstract> but no column alias support.

  my $sabs=SQL::Abstract->new;
  my $sth=$dbx->sqlcursor($sabs->select($table, \@columns, \%where, \@sort));

=cut

sub sqlcursor {
  my $self=shift;
  my $sql=shift;
  my $sth=$self->prepare($sql);
  if (ref($_[0]) eq "ARRAY") {
    my $bind_aref=shift;
    $sth->execute(@$bind_aref) or die(&_error_string($self->errstr, $sql, sprintf("[%s]", join(", ", @$bind_aref)), "Array Reference"));
  } elsif (ref($_[0]) eq "HASH") {
    my $bind_href=shift;
    foreach my $key (keys %$bind_href) {
      next unless $sql=~m/:$key\b/;                #TODO: comments are scanned so /* :foo */ is not supported here
      if (ref($bind_href->{$key}) eq "SCALAR") {
        $sth->bind_param_inout(":$key" => $bind_href->{$key}, 255);
      } else {
        $sth->bind_param(":$key" => $bind_href->{$key});
      }
    }
    $sth->execute or die(&_error_string($self->errstr, $sql, sprintf("{%s}", join(", ", map {join("=>", $_ => $bind_href->{$_})} sort keys %$bind_href)), "Hash Reference"));
  } else {
    my @bind=@_;
    $sth->execute(@bind) or die(&_error_string($self->errstr, $sql, sprintf("(%s)", join(", ", @bind)), "List"));
  }
  return $sth;

  sub _error_string {
    my $err=shift;
    my $sql=shift;
    my $bind_str=shift;
    my $type=shift;
    if ($bind_str) {
      return sprintf("Database Execute Error: %s\nSQL: %s\nBind(%s): %s\n", $err, $sql, $type, $bind_str);
    } else {
      return sprintf("Database Prepare Error: %s\nSQL: %s\n", $err, $sql);
    }
  }
}

=head2 sqlscalar

Returns the first row first column value as a scalar.

This works great for selecting one value.

  my $scalar=$dbx->sqlscalar($sql,  @parameters); #returns $
  my $scalar=$dbx->sqlscalar($sql, \@parameters); #returns $
  my $scalar=$dbx->sqlscalar($sql, \%parameters); #returns $

=cut

sub sqlscalar {
  my $self=shift;
  my @data=$self->sqlarray(@_);
  return $data[0];
}

=head2 sqlarray

Returns the SQL result as an array or array reference.

This works great for selecting one column from a table or selecting one row from a table.

  my $array=$dbx->sqlarray($sql,  @parameters); #returns [$,$,$,...]
  my @array=$dbx->sqlarray($sql,  @parameters); #returns ($,$,$,...)
  my $array=$dbx->sqlarray($sql, \@parameters); #returns [$,$,$,...]
  my @array=$dbx->sqlarray($sql, \@parameters); #returns ($,$,$,...)
  my $array=$dbx->sqlarray($sql, \%parameters); #returns [$,$,$,...]
  my @array=$dbx->sqlarray($sql, \%parameters); #returns ($,$,$,...)

=cut

sub sqlarray {
  my $self=shift;
  my $rows=$self->sqlarrayarray(@_);
  my @rows=map {@$_} @$rows;
  return wantarray ? @rows : \@rows;
}

=head2 sqlhash

Returns the first two columns of the SQL result as a hash or hash reference {Key=>Value, Key=>Value, ...}

  my $hash=$dbx->sqlhash($sql,  @parameters); #returns {$=>$, $=>$, ...}
  my %hash=$dbx->sqlhash($sql,  @parameters); #returns ($=>$, $=>$, ...)
  my @hash=$dbx->sqlhash($sql,  @parameters); #this is ordered
  my @keys=grep {!($n++ % 2)} @hash;          #ordered keys

  my $hash=$dbx->sqlhash($sql, \@parameters); #returns {$=>$, $=>$, ...}
  my %hash=$dbx->sqlhash($sql, \@parameters); #returns ($=>$, $=>$, ...)
  my $hash=$dbx->sqlhash($sql, \%parameters); #returns {$=>$, $=>$, ...}
  my %hash=$dbx->sqlhash($sql, \%parameters); #returns ($=>$, $=>$, ...)

=cut

sub sqlhash {
  my $self=shift;
  my $rows=$self->sqlarrayarray(@_);
  my @rows=map {$_->[0], $_->[1]} @$rows;
  return wantarray ? @rows : {@rows};
}

=head2 sqlarrayarray

Returns the SQL result as an array or array ref of array references ([],[],...) or [[],[],...]

  my $array=$dbx->sqlarrayarray($sql,  @parameters); #returns [[$,$,...],[],[],...]
  my @array=$dbx->sqlarrayarray($sql,  @parameters); #returns ([$,$,...],[],[],...)
  my $array=$dbx->sqlarrayarray($sql, \@parameters); #returns [[$,$,...],[],[],...]
  my @array=$dbx->sqlarrayarray($sql, \@parameters); #returns ([$,$,...],[],[],...)
  my $array=$dbx->sqlarrayarray($sql, \%parameters); #returns [[$,$,...],[],[],...]
  my @array=$dbx->sqlarrayarray($sql, \%parameters); #returns ([$,$,...],[],[],...)

=cut

sub sqlarrayarray {
  my $self=shift;
  my $sql=shift;
  return $self->_sqlarrayarray(sql=>$sql, param=>[@_], name=>0);
}

=head2 sqlarrayarrayname

Returns the SQL result as an array or array ref of array references ([],[],...) or [[],[],...] where the first row contains an array reference to the column names

  my $array=$dbx->sqlarrayarrayname($sql,  @parameters); #returns [[$,$,...],[]...]
  my @array=$dbx->sqlarrayarrayname($sql,  @parameters); #returns ([$,$,...],[]...)
  my $array=$dbx->sqlarrayarrayname($sql, \@parameters); #returns [[$,$,...],[]...]
  my @array=$dbx->sqlarrayarrayname($sql, \@parameters); #returns ([$,$,...],[]...)
  my $array=$dbx->sqlarrayarrayname($sql, \%parameters); #returns [[$,$,...],[]...]
  my @array=$dbx->sqlarrayarrayname($sql, \%parameters); #returns ([$,$,...],[]...)

Create an HTML table with L<CGI>

  my $cgi=CGI->new;
  my $html=$cgi->table($cgi->Tr([map {$cgi->td($_)} $dbx->sqlarrayarrayname($sql, @param)]));

=cut

sub sqlarrayarrayname {
  my $self=shift;
  my $sql=shift;
  return $self->_sqlarrayarray(sql=>$sql, param=>[@_], name=>1);
}

# _sqlarrayarray
#
# my $array=$dbx->_sqlarrayarray(sql=>$sql, param=>[ @parameters], name=>1);
# my @array=$dbx->_sqlarrayarray(sql=>$sql, param=>[ @parameters], name=>1);
# my $array=$dbx->_sqlarrayarray(sql=>$sql, param=>[ @parameters], name=>0);
# my @array=$dbx->_sqlarrayarray(sql=>$sql, param=>[ @parameters], name=>0);
#
# my $array=$dbx->_sqlarrayarray(sql=>$sql, param=>[\@parameters], name=>1);
# my @array=$dbx->_sqlarrayarray(sql=>$sql, param=>[\@parameters], name=>1);
# my $array=$dbx->_sqlarrayarray(sql=>$sql, param=>[\@parameters], name=>0);
# my @array=$dbx->_sqlarrayarray(sql=>$sql, param=>[\@parameters], name=>0);
#
# my $array=$dbx->_sqlarrayarray(sql=>$sql, param=>[\%parameters], name=>1);
# my @array=$dbx->_sqlarrayarray(sql=>$sql, param=>[\%parameters], name=>1);
# my $array=$dbx->_sqlarrayarray(sql=>$sql, param=>[\%parameters], name=>0);
# my @array=$dbx->_sqlarrayarray(sql=>$sql, param=>[\%parameters], name=>0);

sub _sqlarrayarray {
  my $self=shift;
  my %data=@_;
  my $sth=$self->sqlcursor($data{'sql'}, @{$data{'param'}}) or die($self->errstr);
  my $name=$sth->{'NAME'}; #DBD::mysql must store this first
  my $row=[];
  my @rows=();
  #TODO: replace with fetchall_arrayref
  while ($row=$sth->fetchrow_arrayref()) {
    push @rows, [@$row];
  }
  unshift @rows, $name if $data{'name'};
  $sth->finish;
  return wantarray ? @rows : \@rows;
}

=head2 sqlarrayhash

Returns the SQL result as an array or array ref of hash references ({},{},...) or [{},{},...]

  my $array=$dbx->sqlarrayhash($sql,  @parameters); #returns [{},{},{},...]
  my @array=$dbx->sqlarrayhash($sql,  @parameters); #returns ({},{},{},...)
  my $array=$dbx->sqlarrayhash($sql, \@parameters); #returns [{},{},{},...]
  my @array=$dbx->sqlarrayhash($sql, \@parameters); #returns ({},{},{},...)
  my $array=$dbx->sqlarrayhash($sql, \%parameters); #returns [{},{},{},...]
  my @array=$dbx->sqlarrayhash($sql, \%parameters); #returns ({},{},{},...)

This method is best used to select a list of hashes out of the database to bless directly into a package.

  my $sql=q{SELECT COL1 AS "id", COL2 AS "name" FROM TABLE1};
  my @objects=map {bless $_, MyPackage} $dbx->sqlarrayhash($sql,  @parameters);
  my @objects=map {MyPackage->new(%$_)} $dbx->sqlarrayhash($sql,  @parameters);

The @objects array is now a list of blessed MyPackage objects.

=cut

sub sqlarrayhash {
  my $self=shift;
  my $sql=shift;
  return $self->_sqlarrayhash(sql=>$sql, param=>[@_], name=>0);
}

=head2 sqlarrayhashname

Returns the SQL result as an array or array ref of hash references ([],{},{},...) or [[],{},{},...] where the first row contains an array reference to the column names

  my $array=$dbx->sqlarrayhashname($sql,  @parameters); #returns [[],{},{},...]
  my @array=$dbx->sqlarrayhashname($sql,  @parameters); #returns ([],{},{},...)
  my $array=$dbx->sqlarrayhashname($sql, \@parameters); #returns [[],{},{},...]
  my @array=$dbx->sqlarrayhashname($sql, \@parameters); #returns ([],{},{},...)
  my $array=$dbx->sqlarrayhashname($sql, \%parameters); #returns [[],{},{},...]
  my @array=$dbx->sqlarrayhashname($sql, \%parameters); #returns ([],{},{},...)

=cut

sub sqlarrayhashname {
  my $self=shift;
  my $sql=shift;
  return $self->_sqlarrayhash(sql=>$sql, param=>[@_], name=>1);
}

# _sqlarrayhash
#
# Returns the SQL result as an array or array ref of hash references ({},{},...) or [{},{},...]
#
# my $array=$dbx->_sqlarrayhash(sql=>$sql, param=>\@parameters, name=>1);
# my @array=$dbx->_sqlarrayhash(sql=>$sql, param=>\@parameters, name=>1);
# my $array=$dbx->_sqlarrayhash(sql=>$sql, param=>\@parameters, name=>0);
# my @array=$dbx->_sqlarrayhash(sql=>$sql, param=>\@parameters, name=>0);

sub _sqlarrayhash {
  my $self=shift;
  my %data=@_;
  my $sth=$self->sqlcursor($data{'sql'}, @{$data{'param'}}) or die($self->errstr);
  my $name=$sth->{'NAME'}; #DBD::mysql must store this first
  my $row=[];
  my @rows=();
  while ($row=$sth->fetchrow_hashref()) {
    push @rows, {%$row};
  }
  unshift @rows, $name if $data{'name'};
  $sth->finish;
  return wantarray ? @rows : \@rows;
}

=head2 sqlarrayobject

Returns the SQL result as an array of blessed hash objects in to the $class namespace.

  my $array=$dbx->sqlarrayobject($class, $sql,  @parameters); #returns (bless({}, $class), ...)
  my @array=$dbx->sqlarrayobject($class, $sql,  @parameters); #returns [bless({}, $class), ...]
  my ($object)=$dbx->sqlarrayobject($class, $sql,  {id=>$id}); #$object is bless({}, $class)

=cut

sub sqlarrayobject {
  my $self=shift;
  my $class=shift or die("Error: The sqlarrayobject method requires a class parameter");
  my @objects=map {bless($_, $class)} $self->sqlarrayhash(@_);
  wantarray ? @objects : \@objects;
}

=head2 sqlsort (Oracle Specific?)

Returns the SQL statement with the correct ORDER BY clause given a SQL statement (without an ORDER BY clause) and a signed integer on which column to sort.

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

Returns a sqlarrayarrayname for $sql sorted on column $n where n is an integer ascending for positive, descending for negative, and 0 for no sort.

  my $data=$dbx->sqlarrayarraynamesort($sql, $n,  @parameters);
  my $data=$dbx->sqlarrayarraynamesort($sql, $n, \@parameters);
  my $data=$dbx->sqlarrayarraynamesort($sql, $n, \%parameters);

Note: $sql must not have an "ORDER BY" clause in order for this function to work correctly.

=cut

sub sqlarrayarraynamesort {
  my $self=shift;
  my $sql=shift;
  my $sort=shift;
  return $self->sqlarrayarrayname($self->sqlsort($sql, $sort), @_);
}

=head1 METHODS (Read) - SQL::Abstract

=head2 abscursor

Returns the prepared and executed SQL cursor.

  my $sth=$dbx->abscursor($table, \@columns, \%where, \@order);

=cut

sub abscursor {
  my $self=shift;
  return $self->sqlcursor($self->abs->select(@_));
}

=head2 absscalar

Returns the first row first column value as a scalar.

  my $scalar=$dbx->absscalar($table, \@columns, \%where, \@order); #returns $

=cut

sub absscalar {
  my $self=shift;
  return $self->sqlscalar($self->abs->select(@_));
}

=head2 absarray

Returns the SQL result as a array.

This works great for selecting one value.

  my @array=$dbx->absarray($table, \@columns, \%where, \@order); #returns ()
  my $array=$dbx->absarray($table, \@columns, \%where, \@order); #returns []

=cut

sub absarray {
  my $self=shift;
  return $self->sqlarray($self->abs->select(@_));
}

=head2 abshash

Returns the first two columns of the SQL result as a hash or hash reference {Key=>Value, Key=>Value, ...}

  my $hash=$dbx->abshash($table, \@columns, \%where, \@order); #returns {}
  my %hash=$dbx->abshash($table, \@columns, \%where, \@order); #returns ()

=cut

sub abshash {
  my $self=shift;
  return $self->sqlhash($self->abs->select(@_));
}

=head2 absarrayarray

Returns the SQL result as an array or array ref of array references ([],[],...) or [[],[],...]

  my $array=$dbx->absarrayarray($table, \@columns, \%where, \@order); #returns [[$,$,...],[],[],...]
  my @array=$dbx->absarrayarray($table, \@columns, \%where, \@order); #returns ([$,$,...],[],[],...)

=cut

sub absarrayarray {
  my $self=shift;
  return $self->sqlarrayarray($self->abs->select(@_));
}

=head2 absarrayarrayname

Returns the SQL result as an array or array ref of array references ([],[],...) or [[],[],...] where the first row contains an array reference to the column names

  my $array=$dbx->absarrayarrayname($table, \@columns, \%where, \@order); #returns [[$,$,...],[],[],...]
  my @array=$dbx->absarrayarrayname($table, \@columns, \%where, \@order); #returns ([$,$,...],[],[],...)

=cut

sub absarrayarrayname {
  my $self=shift;
  return $self->sqlarrayarrayname($self->abs->select(@_));
}

=head2 absarrayhash

Returns the SQL result as an array or array ref of hash references ({},{},...) or [{},{},...]

  my $array=$dbx->absarrayhash($table, \@columns, \%where, \@order); #returns [{},{},{},...]
  my @array=$dbx->absarrayhash($table, \@columns, \%where, \@order); #returns ({},{},{},...)

=cut

sub absarrayhash {
  my $self=shift;
  return $self->sqlarrayhash($self->abs->select(@_));
}

=head2 absarrayhashname

Returns the SQL result as an array or array ref of hash references ({},{},...) or [{},{},...]

  my $array=$dbx->absarrayhashname($table, \@columns, \%where, \@order); #returns [[],{},{},...]
  my @array=$dbx->absarrayhashname($table, \@columns, \%where, \@order); #returns ([],{},{},...)

=cut

sub absarrayhashname {
  my $self=shift;
  return $self->sqlarrayhashname($self->abs->select(@_));
}

=head2 absarrayobject

Returns the SQL result as an array of blessed hash objects in to the $class namespace.

  my $array=$dbx->absarrayobject($class, $table, \@columns, \%where, \@order); #returns (bless({}, $class), ...)
  my @array=$dbx->absarrayobject($class, $table, \@columns, \%where, \@order); #returns [bless({}, $class), ...]

=cut

sub absarrayobject {
  my $self=shift;
  my $class=shift or die("Error: The absarrayobject method requires a class parameter");
  my @objects=map {bless($_, $class)} $self->absarrayhash(@_);
  wantarray ? @objects : \@objects;
}

=head1 METHODS (Write) - SQL

Remember to commit or use AutoCommit

Note: It appears that some drivers do not support the count of rows.

=head2 sqlinsert, insert

Returns the number of rows inserted by the SQL statement.

  my $count=$dbx->sqlinsert( $sql,   @parameters);
  my $count=$dbx->sqlinsert( $sql,  \@parameters);
  my $count=$dbx->sqlinsert( $sql,  \%parameters);

=cut

*sqlinsert=\&update;

*insert=\&update;

=head2 sqlupdate, update

Returns the number of rows updated by the SQL statement.

  my $count=$dbx->sqlupdate( $sql,   @parameters);
  my $count=$dbx->sqlupdate( $sql,  \@parameters);
  my $count=$dbx->sqlupdate( $sql,  \%parameters);

=cut

*sqlupdate=\&update;

sub update {
  my $self=shift;
  my $sql=shift;
  my $sth=$self->sqlcursor($sql, @_) or die($self->errstr);
  my $rows=$sth->rows;
  $sth->finish;
  return $rows;
}

=head2 sqldelete, delete

Returns the number of rows deleted by the SQL statement.

  my $count=$dbx->sqldelete($sql,   @parameters);
  my $count=$dbx->sqldelete($sql,  \@parameters);
  my $count=$dbx->sqldelete($sql,  \%parameters);

Note: Some Oracle clients do not support row counts on delete instead the value appears to be a success code.

=cut

*sqldelete=\&update;

*delete=\&update;

=head2 execute, exec

Executes stored procedures.

  my $out;
  my $return=$dbx->execute($sql, $in, \$out);            #pass in/out vars as scalar reference
  my $return=$dbx->execute($sql, [$in, \$out]);
  my $return=$dbx->execute($sql, {in=>$in, out=>\$out});

Note: Currently sqlupdate, sqlinsert, sqldelete, and execute all point to the same method.  This may change in the future if we need to change the behavior of one method.  So, please use the correct method name for your function.

=cut

*execute=\&update;
*exec=\&update;   #deprecated

=head1 METHODS (Write) - SQL::Abstract

=head2 absinsert

Returns the number of rows inserted.

  my $count=$dbx->absinsert($table, \%column_values);

=cut

sub absinsert {
  my $self=shift;
  return $self->sqlinsert($self->abs->insert(@_));
}

=head2 absupdate

Returns the number of rows updated.

  my $count=$dbx->absupdate($table, \%column_values, \%where);

=cut

sub absupdate {
  my $self=shift;
  return $self->sqlupdate($self->abs->update(@_));
}

=head2 absdelete

Returns the number of rows deleted.

  my $count=$dbx->absdelete($table, \%where);

=cut

sub absdelete {
  my $self=shift;
  return $self->sqldelete($self->abs->delete(@_));
}

=head1 METHODS (Write) - Bulk - SQL

=head2 bulksqlinsertarrayarray

Insert records in bulk.

  my @arrayarray=(
                  [data1, $data2, $data3, $data4, ...],
                  [@row_data_2],
                  [@row_data_3], ...
                 );
  my $count=$dbx->bulksqlinsertarrayarray($sql, \@arrayarray);

=cut

sub bulksqlinsertarrayarray {
  my $self         = shift;
  my $sql          = shift or die('Error: sql required.');
  my $arrayarray   = shift or die('Error: array of array references required.');
  my $sth          = $self->prepare($sql);
  my $size         = @$arrayarray;
  my @tuple_status = ();
  my $count        = $sth->execute_for_fetch( sub {shift @$arrayarray}, \@tuple_status);
  unless ($count == $size) {
    warn map {"$_\n"} @tuple_status; #TODO better error trapping...
  }
  return $count;
}

=head2 bulksqlinsertcursor

Insert records in bulk.

Step 1 select data from table 1 in database 1

  my $sth1=$dbx1->sqlcursor('Select Col1 AS "ColA", Col2 AS "ColB", Col3 AS "ColC" from table1');

Step 2 insert in to table 2 in database 2

  my $count=$dbx2->bulksqlinsertcursor($sql, $sth1);

Note: If you are inside a single database, it is much more efficient to use insert from select syntax. As there is no need for data to be transferred between the server and the client.

=cut

sub bulksqlinsertcursor {
  my $self         = shift;
  my $sql          = shift or die('Error: sql required.');
  my $cursor       = shift or die('Error: cursor required.');
  my $sth          = $self->prepare($sql);
  my @tuple_status = ();
  my $size         = 0;
  my $count        = $sth->execute_for_fetch( sub {my $row=$cursor->fetchrow_arrayref; $size++ if $row; return $row}, \@tuple_status);
  unless ($count == $size) {
    warn Dumper \@tuple_status; #TODO better error trapping...
  }
  return $count;
}

=head2 bulksqlupdatearrayarray

Update records in bulk.

  my @arrayarray   = (
                      [$data1, $data2, $data3, $data4, $id],
                      [@row_data_2],
                      [@row_data_3], ...
                     );
  my $count        = $dbx->bulksqlupdatearrayarray($sql, \@arrayarray);

=cut

sub bulksqlupdatearrayarray {
  my $self         = shift;
  my $sql          = shift or die('Error: sql required.');
  my $arrayarray   = shift or die('Error: array of array references required.');
  my $sth          = $self->prepare($sql);
  my $size         = @$arrayarray;
  my @tuple_status = ();
  my $noerror      = $sth->execute_for_fetch( sub {shift @$arrayarray}, \@tuple_status);
  warn("Warning: Atempted $size updates but only $noerror where successful.") unless $size == $noerror;
  my $count        = sum(0, grep {$_ > 0} @tuple_status);
  return $count;
}

=head1 METHODS (Write) - Bulk - SQL::Abstract-like

These bulk methods do not use L<SQL::Abstract> but our own similar SQL insert and update methods.

=head2 bulkabsinsertarrayarray

Insert records in bulk.

  my @columns=("Col1", "Col2", "Col3", "Col4", ...);
  my @arrayarray=(
                  [data1, $data2, $data3, $data4, ...],
                  [@row_data_2],
                  [@row_data_3], ...
                 );
  my $count=$dbx->bulkabsinsertarrayarray($table, \@columns, \@arrayarray);

=cut

sub bulkabsinsertarrayarray {
  my $self         = shift;
  my $table        = shift or die('Error: table name required.');
  my $columns      = shift or die('Error: columns array reference required.');
  my $arrayarray   = shift or die('Error: array of array references required.');
  my $sql          = $self->_bulkinsert_sql($table => $columns);
  return $self->bulksqlinsertarrayarray($sql, $arrayarray);
}

=head2 bulkabsinsertarrayhash

Insert records in bulk.

  my @columns=("Col1", "Col2", "Col3", "Col4", ...);                           #case sensative with respect to @arrayhash
  my @arrayhash=(
                 {C0l1=>data1, Col2=>$data2, Col3=>$data3, Col4=>$data4, ...}, #extra hash items ignored when sliced using @columns
                 \%row_hash_data_2,
                 \%row_hash_data_3, ...
                );
  my $count=$dbx->bulkabsinsertarrayhash($table, \@columns, \@arrayhash);

=cut

sub bulkabsinsertarrayhash {
  my $self       = shift;
  my $table      = shift or die("Error: table name required.");
  my $columns    = shift or die("Error: columns array reference required.");
  my $arrayhash  = shift or die("Error array of hash references required");
  my @arrayarray = map {my %hash=%$_; my @slice=@hash{@$columns}; \@slice} @$arrayhash;
  return $self->bulkabsinsertarrayarray($table, $columns, \@arrayarray);
}

=head2 bulkabsinsertcursor

Insert records in bulk.

Step 1 select data from table 1 in database 1

  my $sth1=$dbx1->sqlcursor('Select Col1 AS "ColA", Col2 AS "ColB", Col3 AS "ColC" from table1');

Step 2 insert in to table 2 in database 2

  my $count=$dbx2->bulkabsinsertcursor($table2, $sth1);

  my $count=$dbx2->bulkabsinsertcursor($table2, \@columns, $sth1); #if your DBD/API does not support column alias support

Note: If you are inside a single database, it is much more efficient to use insert from select syntax. As no data needs to be transferred to and from the client.

=cut

sub bulkabsinsertcursor {
  my $self         = shift;
  my $table        = shift or die('Error: table name required.');
  my $cursor       = pop   or die('Error: cursor required.');
  my $columns      = shift || $cursor->{'NAME'};
  my $sql          = $self->_bulkinsert_sql($table => $columns);
  return $self->bulksqlinsertcursor($sql, $cursor);
}

#head2 _bulkinsert_sql
#
#Our own method since SQL::Abstract does not support ordered column values
#
#cut

sub _bulkinsert_sql {
  my $self=shift;
  my $table=shift;
  my $columns=shift;
  my $sql=sprintf("INSERT INTO $table (%s) VALUES (%s)", join(',', @$columns), join(',', map {'?'} @$columns));
  #warn "$sql\n";
  return $sql;
}

=head2 bulkabsupdatearrayarray

Update records in bulk.

  my @setcolumns   = ("Col1", "Col2", "Col3", "Col4");
  my @wherecolumns = ("ID");
  my @arrayarray   = (
                      [$data1, $data2, $data3, $data4, $id],
                      [@row_data_2],
                      [@row_data_3], ...
                     );
  my $count        = $dbx->bulkabsupdatearrayarray($table, \@setcolumns, \@wherecolumns, \@arrayarray);

=cut

sub bulkabsupdatearrayarray {
  my $self         = shift;
  my $table        = shift or die('Error: table name required.');
  my $setcolumns   = shift or die('Error: set columns array reference required.');
  my $wherecolumns = shift or die('Error: where columns array reference required.');
  my $arrayarray   = shift;
  my $sql          = $self->_bulkupdate_sql($table => $setcolumns, $wherecolumns);
  return $self->bulksqlupdatearrayarray($sql, $arrayarray);
}

#head2 _bulkinsert_sql
#
#Our own method since SQL::Abstract does not support ordered column values
#
##cut

sub _bulkupdate_sql {
  my $self=shift;
  my $table=shift;
  my $setcolumns=shift;
  my $wherecolumns=shift;
  my $sql=sprintf("UPDATE $table SET %s WHERE %s", join(", ", map {"$_ = ?"} @$setcolumns), join(" AND ", map {"$_ = ?"} @$wherecolumns));
  #warn "$sql\n";
  return $sql;
}

=head1 Constructors

=head2 abs

Returns a L<SQL::Abstract> object

=cut

sub abs {
  my $self=shift;
  $self->{'abs'}=shift if @_;
  unless (defined $self->{'abs'}) {
    eval 'use SQL::Abstract'; #run time require so as not to require installation for all users
    my $error=$@;
    die($error) if $error;
    $self->{'abs'}=SQL::Abstract->new;
  }
  return $self->{'abs'};
}

=head1 Methods (Informational)

=head2 dbms_name

Return the DBMS Name (e.g. Oracle, MySQL, PostgreSQL)

=cut

sub dbms_name {shift->dbh->get_info(17)};

=head1 Methods (Session Management)

These methods allow the setting of Oracle session features that are available in the v$session table.  If other databases support these features, please let me know.  But, as it stands, these method are non operational unless SQL_DBMS_NAME is Oracle.

=head2 module

Sets and returns the v$session.module (Oracle) value.

Note: Module is set for you by DBD::Oracle.  However you may set it however you'd like.  It should be set once after connection and left alone.

  $dbx->module("perl@host");      #normally set by DBD::Oracle
  $dbx->module($module, $action); #can set initial action too.
  my $module=$dbx->module();

=cut

sub module {
  my $self=shift;
  return unless $self->dbms_name eq 'Oracle';
  if (@_) {
    my $module=shift;
    my $action=shift;
    $self->execute($self->_set_module_sql, $module, $action);
  }
  if (defined wantarray) {
    return $self->sqlscalar($self->_sys_context_userenv_sql, 'MODULE');
  } else {
    return; #void context no need to hit the database
  }
}

sub _set_module_sql {
  return qq{
            --Script: $0
            --Package: $PACKAGE
            --Method: _set_module_action_sql
            BEGIN
              DBMS_APPLICATION_INFO.set_module(module_name => ?, action_name => ?);
            END;
           };
}

=head2 client_info

Sets and returns the v$session.client_info (Oracle) value.

  $dbx->client_info("Running From crontab");
  my $client_info=$dbx->client_info();

You may use this field for anything up to 64 characters!

  $dbx->client_info(join "~", (ver => 4, realm => "ldap", grp =>25)); #tilde is a fairly good separator
  my %client_info=split(/~/, $dbx->client_info());

=cut

sub client_info {
  my $self=shift;
  return unless $self->dbms_name eq 'Oracle';
  if (@_) {
    my $text=shift;
    $self->execute($self->_set_client_info_sql, $text);
  }
  if (defined wantarray) {
    return $self->sqlscalar($self->_sys_context_userenv_sql, 'CLIENT_INFO');
  } else {
    return; #void context no need to hit the database
  }
}

sub _set_client_info_sql {
  return qq{
            --Script: $0
            --Package: $PACKAGE
            --Method: _action_sql
            BEGIN
              DBMS_APPLICATION_INFO.set_client_info(client_info => ?);
            END;
           };
}

=head2 action

Sets and returns the v$session.action (Oracle) value.

  $dbx->action("We are Here");
  my $action=$dbx->action();

Note: This should be updated fairly often. Every loop if it runs for more than 5 seconds and may end up in V$SQL_MONITOR.

  while ($this) {
    local $dbx->{'action'}="This Loop"; #tied to the database with a little Perl sugar
  }

=cut

sub action {
  my $self=shift;
  return unless $self->dbms_name eq 'Oracle';
  if (@_) {
    my $text=shift;
    $self->execute($self->_set_action_sql, $text);
  }
  if (defined wantarray) {
    return $self->sqlscalar($self->_sys_context_userenv_sql, 'ACTION');
  } else {
    return; #void context no need to hit the database
  }
}

sub _set_action_sql {
  return qq{
            --Script: $0
            --Package: $PACKAGE
            --Method: _action_sql
            BEGIN
              DBMS_APPLICATION_INFO.set_action(action_name => ?);
            END;
           };
}

=head2 client_identifier

Sets and returns the v$session.client_identifier (Oracle) value.

  $dbx->client_identifier($login);
  my $client_identifier = $dbx->client_identifier();

Note: This should be updated based on the login of the authenticated end user.  I use the client_info->{'realm'} if you have more than one authentication realm.

For auditing add this to an update trigger

  new.UPDATED_USER = sys_context('USERENV', 'CLIENT_IDENTIFIER');

=cut

sub client_identifier {
  my $self=shift;
  return unless $self->dbms_name eq 'Oracle';
  if (@_) {
    my $text=shift;
    $self->execute($self->_set_client_identifier_sql, $text);
  }
  if (defined wantarray) {
    return $self->sqlscalar($self->_sys_context_userenv_sql, 'CLIENT_IDENTIFIER');
  } else {
    return; #void context no need to hit the database
  }
}

sub _set_client_identifier_sql {
  return qq{
            --Script: $0
            --Package: $PACKAGE
            --Method: _client_identifier_sql
            BEGIN
              DBMS_SESSION.SET_IDENTIFIER(client_id => ?);
            END;
           };
}

sub _sys_context_userenv_sql {
  return qq{
            --Script: $0
            --Package: $PACKAGE
            SELECT sys_context('USERENV',?)
              FROM SYS.DUAL
           };
}

=head1 TODO

Sort functions sqlsort and sqlarrayarraynamesort may not be portable.

Add some kind of capability to allow hash binds to bind as some native type rather than all strings.

Hash binds scan comments for bind variables e.g. /* :variable */

Improve error messages

=head1 BUGS

Send email to author and log on RT.

=head1 SUPPORT

DavisNetworks.com supports all Perl applications including this package.

=head1 AUTHOR

  Michael R. Davis
  CPAN ID: MRDVT
  STOP, LLC
  domain=>stopllc,tld=>com,account=>mdavis
  http://www.stopllc.com/

=head1 COPYRIGHT

This program is free software licensed under the...

  The BSD License

The full text of the license can be found in the LICENSE file included with this module.

=head1 SEE ALSO

=head2 The Competition

L<DBIx::DWIW>, L<DBIx::Wrapper>, L<DBIx::Simple>, L<Data::Table::fromSQL>, L<DBIx::Wrapper::VerySimple>, L<DBIx::Raw>

=head2 The Building Blocks

L<DBI>, L<SQL::Abstract>

=cut

1;
