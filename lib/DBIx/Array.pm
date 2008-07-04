package DBIx::Array;
use strict;
use DBI;

BEGIN {
  use vars qw($VERSION);
  $VERSION     = '0.01';
}

=head1 NAME

DBIx::Array - This modules is a wrapper around DBI with array interfaces

=head1 SYNOPSIS

  use DBIx::Array;
  $dba->connect($connection, $user, $pass, \%opt); #passed to DBI
  my @array=$dba->sqlarray($sql, @params);

=head1 DESCRIPTION

=head1 USAGE

=head1 CONSTRUCTOR

=head2 new

  my $dba = DBIx::Array->new();

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

=head2 connect

Connects to the database and returns the database handle.

  $dba->connect($connection, $user, $pass, \%opt);

=cut

sub connect {
  my $self=shift();
  my $dbh=DBI->connect(@_);
  return $self->dbh($dbh);
}

=head2 disconnect

Calls $dbh->disconnect

  $dba->disconnect;

=cut

sub disconnect {
  my $self=shift;
  return $self->dbh->disconnect
}

=head2 commit

=cut

sub commit {
  my $self=shift;
  return $self->dbh->commit;
}

=head2 rollback

=cut

sub rollback {
  my $self=shift;
  return $self->dbh->rollback;
}

=head2 AutoCommit

=cut

sub AutoCommit {
  my $self=shift;
  if (@_) {
    $self->dbh->{'AutoCommit'}=shift;
  }
  return $self->dbh->{'AutoCommit'};
}

=head2 RaiseError

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

=cut

sub errstr {$DBI::errstr};

=head2 dbh

Sets or returns the database handle object.

  $dba->dbh;
  $dba->dbh($dbh);  #if you don't use DBI.

=cut

sub dbh {
  my $self = shift();
  $self->{'dbh'}=shift() if @_;
  return $self->{'dbh'};
}

=head2 sqlcursor

Returns the SQL cursor so that you can use the cursor elsewhere.

=cut

sub sqlcursor {
  my $self=shift;
  my $sql=shift;
  my $sth=$self->dbh->prepare($sql)    or die($self->errstr);
  $sth->execute(@_)                    or die($self->errstr);
  return $sth;
}

=head2 sqlscalar

Returns the SQL query as a scalar.

This works great for selecting one value.

  $scalar=$dba->sqlscalar($sql, @parameters);

=cut

sub sqlscalar {
  my $self=shift();
  my @data=$self->sqlarray(@_);
  return $data[0];
}

=head2 sqlarray

Returns the SQL query as an array or array reference.

This works great for selecting one column from a table or selecting one row from a table.

  $array=$dba->sqlarray($sql, @parameters);
  @array=$dba->sqlarray($sql, @parameters);

=cut

sub sqlarray {
  my $self=shift();
  my $rows=$self->sqlarrayarray(@_);
  my @rows=map {@$_} @$rows;
  return wantarray ? @rows : \@rows;
}

=head2 sqlhash

Returns the first two columns of the SQL query as a hash or hash reference {Key=>Value, Key=>Value, ...}

  $hash=$dba->sqlhash($sql, @parameters);
  %hash=$dba->sqlhash($sql, @parameters);
  @hash=$dba->sqlhash($sql, @parameters); #this is ordered
  @keys=grep {!($n++ % 2)} @hash;         #ordered keys

=cut

sub sqlhash {
  my $self=shift();
  my $rows=$self->sqlarrayarray(@_);
  my @rows=map {$_->[0], $_->[1]} @$rows;
  return wantarray ? @rows : {@rows};
}

=head2 sqlarrayarray

Returns the SQL data as an array or array ref of array references ([],[],...) or [[],[],...]

  $array=$dba->sqlarrayarray($sql, @parameters);
  @array=$dba->sqlarrayarray($sql, @parameters);

=cut

sub sqlarrayarray {
  my $self=shift();
  my $sql=shift();
  return $self->_sqlarrayarray(sql=>$sql, param=>[@_], name=>0);
}

=head2 sqlarrayarrayname

Returns the SQL data as an array or array ref of array references ([],[],...) or [[],[],...] where the first rows is the column names

  $array=$dba->sqlarrayarrayname($sql, @parameters);
  @array=$dba->sqlarrayarrayname($sql, @parameters);

=cut

sub sqlarrayarrayname {
  my $self=shift();
  my $sql=shift();
  return $self->_sqlarrayarray(sql=>$sql, param=>[@_], name=>1);
}

=head2 _sqlarrayarray

  $array=$dba->_sqlarrayarray(sql=>$sql, param=>\@parameters, name=>1);
  @array=$dba->_sqlarrayarray(sql=>$sql, param=>\@parameters, name=>1);
  $array=$dba->_sqlarrayarray(sql=>$sql, param=>\@parameters, name=>0);
  @array=$dba->_sqlarrayarray(sql=>$sql, param=>\@parameters, name=>0);

=cut

sub _sqlarrayarray {
  my $self=shift();
  my %data=@_;
  my $sth=$self->sqlcursor($data{'sql'}, @{$data{'param'}}) or die($self->errstr);
  my $row=[];
  my @rows=();
  while ($row=$sth->fetchrow_arrayref()) {
    push @rows, [@$row];
  }
  unshift @rows, $sth->{'NAME'} if $data{'name'};
  $sth->finish;
  return wantarray ? @rows : \@rows;
}

=head2 sqlarrayhash

Returns the SQL data as an array or array ref of hash references ({},{},...) or [{},{},...]

  $array=$dba->sqlarrayhash($sql, @parameters);
  @array=$dba->sqlarrayhash($sql, @parameters);

=cut

sub sqlarrayhash {
  my $self=shift();
  my $sql=shift();
  return $self->_sqlarrayhash(sql=>$sql, param=>[@_], name=>0);
}

=head2 sqlarrayhashname

Returns the SQL of data as an array or array ref of hash references ([],{},{},...) or [[],{},{},...] where the first rows is an array reference of the column names

  $array=$dba->sqlarrayhashname($sql, @parameters);
  @array=$dba->sqlarrayhashname($sql, @parameters);

=cut

sub sqlarrayhashname {
  my $self=shift();
  my $sql=shift();
  return $self->_sqlarrayhash(sql=>$sql, param=>[@_], name=>1);
}

=head2 _sqlarrayhash

Returns the SQL data as an array or array ref of hash references ({},{},...) or [{},{},...]

  $array=$dba->_sqlarrayhash(sql=>$sql, param=>\@parameters, name=>1);
  @array=$dba->_sqlarrayhash(sql=>$sql, param=>\@parameters, name=>1);
  $array=$dba->_sqlarrayhash(sql=>$sql, param=>\@parameters, name=>0);
  @array=$dba->_sqlarrayhash(sql=>$sql, param=>\@parameters, name=>0);

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

  my $sql=$dba->sqlsort(qq{SELECT 1,'Z' FROM DUAL UNION SELECT 2,'A' FROM DUAL}, -2);

Returns

  SELECT 1,'Z' FROM DUAL UNION SELECT 2,'A' FROM DUAL ORDER BY 2 DESC

See also the STOP::HTML->sqlarrayarraynamesort function

=cut 

sub sqlsort {
  my $self=shift;
  my $sql=shift;
  my $sort=shift;
  if (defined($sort) and $sort=int($sort)) {
    my $column=abs($sort);
    my $direction = $sort < 0 ? "DESC" : "ASC";
    return join " ", $sql, sprintf("ORDER BY %u %s NULLS LAST", $column, $direction);  
  } else {
    return $sql;
  }
}

=head2 sqlarrayarraynamesort

Returns a sqlarrayarrayname for $sql sorted on column $n where n is an integer asending for positive, desending for negative, 
and 0 for no sort.

  my $data=$dba->sqlarrayarraynamesort($sql, $n, @parameters);

Note: $sql must not have an "ORDER BY" clause in order for this function to work corectly.

=cut

sub sqlarrayarraynamesort {
  my $self=shift;
  my $sql=shift;
  my $sort=shift;
  return $self->sqlarrayarrayname($self->sqlsort($sql, $sort), @_);
} 

=head2 update, delete, exec, execute

Returns the number of rows updated or deleted by the SQL statement.

  $rows=$dba->update($sql, @parameters);
  $rows=$dba->delete($sql, @parameters);

Remember to commit or use AutoCommit

=cut

*delete=\&update;
*exec=\&update;
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

=cut

1;
