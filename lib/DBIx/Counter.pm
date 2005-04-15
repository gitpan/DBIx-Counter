package DBIx::Counter;

use DBI;
use Carp qw(carp croak);
use strict;
use warnings;

require 5.004;

use overload (
               '++'     => \&inc,
               '--'     => \&dec,
               '""'     => \&value,
               fallback => 1,
             );

our ($VERSION, $DSN, $LOGIN, $PASSWORD, $TABLENAME );

$VERSION = '0.01';

=pod

=head1 NAME

DBIx::Counter - Manipulate named counters stored in a database

=head1 WARNING

This is the initial release! It has been tested to work with SQLite, Mysql and MS SQL Server, under perl 5.6 and 5.8. 

I would appreciate feedback, and some help on making it compatible with older versions of perl. I know 'use warnings' and 'our' don't work before 5.6, but that's where my historic knowledge ends.

=head1 SYNOPSIS

    use DBIx::Counter;
    $c = DBIx::Counter->new('my counter', 
                            dsn       => 'dbi:mysql:mydb',
                            login     => 'username',
                            password  => 'secret'
                           );
    $c->inc;
    print $c->value;
    $c->dec;

=head1 DESCRIPTION

This module creates and maintains named counters in a database.
It has a simple interface, with methods to increment and decrement the counter by one, and a method for retrieving the value.
It supports operator overloading for increment (++), decrement (--) and stringification ("").
It should perform well in persistent environments, since it uses the connect_cached and prepare_cached methods of DBI.
The biggest advantage over its main inspiration - L<File::CounterFile> - is that it allows distributed, concurrent access to the counters and isn't tied to a single file system.

Connection settings can be set in the constructor, or by using the package variables $DSN, $LOGIN and $PASSWORD and $TABLENAME.
The table name is configurable, but the column names are currently hard-coded to counter_id and value. The following SQL statement can be used to create the table:

    CREATE TABLE counters (
        counter_id  varchar(64) primary key,
        value       int not null default 0
    );

This module attempts to mimick the File::CounterFile interface, except currently it only supports integer counters.
The locking functions in File::CounterFile are present for compatibility only: they always return 0.

=head1 METHODS

=over

=item new

Creates a new counter instance. 
First parameter is the required counter name. 
Second, optional, argument is an initial value for the counter on its very first use. 
It also accepts named parameters for an already existing database handle, or the dbi connection string, dbi login and dbi password, and the table name:

    dbh         - a pre-existing DBI connection
    dsn         - overrides $DBIx::Counter::DSN
    login       - overrides $DBIx::Counter::LOGIN
    password    - overrides $DBIx::Counter::PASSWORD
    tablename   - overrides $DBIx::Counter::TABLENAME

    Examples:
    $c = DBIx::Counter->new('my counter');
    $c = DBIx::Counter->new('my counter', dbh => $dbh);
    $c = DBIx::Counter->new('my counter', 
                            dsn       => 'dbi:mysql:mydb',
                            login     => 'username',
                            password  => 'secret'
                           );
    $c = DBIx::Counter->new('my counter',
                            42,
                            dsn       => 'dbi:mysql:mydb',
                            tablename => 'gauges'
                           );

=cut

sub new
{
    my $pkg         = shift;
    my $countername = shift or croak("No counter name supplied");
    my $initial     = shift if @_ % 2;
    my %opts        = @_;

    my $self = {
                 countername => $countername,
                 dbh         => $opts{dbh},
                 dsn         => $opts{dsn}       || $DSN,
                 login       => $opts{login}     || $LOGIN,
                 password    => $opts{password}  || $PASSWORD,
                 tablename   => $opts{tablename} || $TABLENAME || 'counters',
                 initial     => $initial         || '0',
               };

    croak("Unable to connect to database: no valid connection handle or valid DSN supplied") unless $self->{dbh} or $self->{dsn};

    bless $self, $pkg;
    $self->_init;
    $self;
}

sub _init
{
    my $self = shift;
    # create counter record if not exists
    eval {
        my $dbh = $self->_db;
        my ($exists) = $dbh->selectrow_array( qq{select count(*) from $self->{tablename} where counter_id=?}, undef, $self->{countername} );
        unless( $exists > 0 ) {
            $dbh->do( qq{insert into $self->{tablename} (counter_id,value) values (?,?)}, undef, $self->{countername}, $self->{initial} ) ;
        }
    } or croak "Error creating counter record: $@";
}

sub _db
{
    my $self = shift;
    return $self->{dbh} || DBI->connect_cached( $self->{dsn}, $self->{login}, $self->{password}, { PrintError => 0, RaiseError => 1 } );
}

sub _add
{
    my ( $self, $add ) = @_;
    my $dbh     = $self->_db;
    my $sth_set = $dbh->prepare_cached(qq{update $self->{tablename} set value=value+? where counter_id=?});
    $sth_set->execute( $add, $self->{countername} );
}

=pod

=item inc

increases the counter by one.

    $c->inc;
    # or using overload:
    $c++;

=cut

sub inc
{
    my $self = shift;
    $self->_add(1);
}

=pod

=item dec

decreases the counter by one.

    $c->dec;
    # or using overload:
    $c--;

=cut

sub dec
{
    my $self = shift;
    $self->_add(-1);
}

=pod

=item value

returns the current value of the counter.

    print $c->value;
    # or using overload: 
    print "Item $c is being processed\n";

=cut

sub value
{
    my $self    = shift;
    my $dbh     = $self->_db;
    my $sth_get = $dbh->prepare_cached(qq{select value from $self->{tablename} where counter_id=?});

    $sth_get->execute( $self->{countername} );
    my ($v) = $sth_get->fetchrow_array;
    $sth_get->finish;

    return $v;
}

=pod

=item lock

Noop. Only provided for API compatibility with File::CounterFile.

=item unlock

Noop. Only provided for API compatibility with File::CounterFile.

=item locked

Noop. Only provided for API compatibility with File::CounterFile.

=cut

sub lock { 0 }
sub unlock { 0 }
sub locked { 0 }

=pod

=back

=head1 SEE ALSO

L<File::CounterFile>

=head1 AUTHOR

Rhesa Rozendaal, E<lt>rhesa@cpan.orgE<gt>. 

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2005 by Rhesa Rozendaal

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.2 or,
at your option, any later version of Perl 5 you may have available.

=cut

1;
