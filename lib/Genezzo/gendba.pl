#!/usr/bin/perl
#
# $Header: /Users/claude/g3/lib/Genezzo/RCS/gendba.pl,v 6.2 2004/08/12 09:46:48 claude Exp claude $
#
# copyright (c) 2003, 2004 Jeffrey I Cohen, all rights reserved, worldwide
#
#
#use strict;
use Genezzo::GenDBI;
use Getopt::Long;
use Pod::Usage;

=head1 NAME

B<gendba.pl> - line mode for Genezzo system

=head1 SYNOPSIS

B<gendba> [options] 

Options:

    -help            brief help message
    -man             full documentation
    -init            build a gnz_home installation if necessary
    -gnz_home        supply a directory for the gnz_home
    -shutdown        do not startup
    -define key=val  define a configuration parameter

=head1 OPTIONS

=over 8

=item B<-help>

    Print a brief help message and exits.

=item B<-man>
    
    Prints the manual page and exits.

=item B<-init>
    
    Build the gnz_home dictionary and default tablespace if 
    it does not exist.

=item B<-gnz_home>
    
    Supply the location for the gnz_home installation.  If 
    specified, it overrides the GNZ_HOME environment variable.

=item B<-shutdown>
    
    If initializing a new database, then shutdown when 
    complete, versus continuing in interactive mode.

=item B<-define> key=value
    
    If initializing a new database, define a configuration 
    parameter.

=back

=head1 DESCRIPTION

Genezzo is an extensible, persistent datastore, that uses a pidgin,
SQL-like syntax.

=head2 Commands

Genezzo understands a SQL-like insert, update, select, and delete,
and it supports the following "short" commands: ct, dt, s, i, d, u

=over 8

=item B<ct - create table>

  example: ct EMP NAME=c ID=n
  SQL equivalent: CREATE TABLE EMP (NAME CHAR(10), ID NUMBER) ;

=item B<dt - drop table>

  example: dt EMP
  SQL equivalent: DROP TABLE EMP ;

=item B<s - select>

  example: s EMP *
  SQL equivalent: SELECT * FROM EMP ;

  example: s EMP rid rownum *
  SQL equivalent: SELECT ROWID, ROWNUM, * FROM EMP ;

  example: s EMP NAME
  SQL equivalent: SELECT NAME FROM EMP ;

=item B<i - insert>

  example: i EMP bob 1 orville 2
  SQL equivalent: 
    INSERT INTO EMP VALUES ('bob', '1');
    INSERT INTO EMP VALUES ('orville', '2'); 


=item B<d, u - delete and update>

  DELETE and UPDATE only work by rid 
  -- you cannot specify a predicate.

  example: d emp 1.2.3
  SQL equivalent: DELETE FROM EMP WHERE RID=1.2.3 ;

  example: u emp 1.2.3 wilbur 4
  SQL equivalent: UPDATE EMP SET NAME='wilbur', 
                                 ID='4' WHERE RID=1.2.3 ;


=back

Genezzo stores information in a couple of subsidiary files: the
default install creates a file called default.dbf which contains the
basic dictionary information.  Other data files can be added as
needed.  While the default configuration uses a single, fixed-size
datafile, Genezzo can be configured to use datafiles that grow to some
maximum size, and it can also be configured to automatically create
new datafiles as necessary.

All tables are currently created in the system tablespace.

There are a couple of other useful commands:

=item HELP -- give useless help

=item DUMP -- dump out internal data structures

=item DUMP TABLES - list all tables

=item DUMP TS - dump tablespace information

=item RELOAD - reload all Genezzo perl modules (will lose uncommited changes, though)

=item COMMIT - force write of changes to database.  Note that even CREATE TABLE is
transactional -- you have to commit to update the persistent dictionary.
Forgetting to commit can cause weird behaviors, since the buffer cache may
flush data out to the dbf file.  Then you can have the condition where the
tablespace reuses these "empty" blocks and they already have data in them.

=head2 Environment

GNZ_HOME: If the user does not specify a gnz_home directory using 
the B<'-gnz_home'> option, Genezzo stores dictionary and table
information in the location specified by this variable.  If 
GNZ_HOME is undefined, the default location is $HOME/gnz_home.

=head1 AUTHORS

Copyright 2003, 2004 Jeffrey I Cohen.  All rights reserved.  

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

Address bug reports and comments to: jcohen@genezzo.com

=cut

    my $glob_init;
    my $glob_gnz_home;
    my $glob_shutdown; 
    my $glob_id;
    my $glob_defs;

sub setinit
{
    $glob_init     = shift;
    $glob_gnz_home = shift;
    $glob_shutdown = shift;
    $glob_defs     = shift;
}

BEGIN {
    my $man  = 0;
    my $help = 0;
    my $init = 0;
    my $shutdown = 0;
    my $gnz_home = '';
    my %defs = ();      # list of --define key=value

    GetOptions(
               'help|?' => \$help, man => \$man, init => \$init,
               shutdown => \$shutdown,
               'gnz_home=s' => \$gnz_home,
               'define=s'   => \%defs)
        or pod2usage(2);

    $glob_id = "Genezzo Version $Genezzo::GenDBI::VERSION - $Genezzo::GenDBI::RELSTATUS $Genezzo::GenDBI::RELDATE\n\n"; 

    pod2usage(-msg => $glob_id, -exitstatus => 1) if $help;
    pod2usage(-msg => $glob_id, -exitstatus => 0, -verbose => 2) if $man;

    setinit($init, $gnz_home, $shutdown, \%defs);

    print "loading...\n" ;
}

my $fb = Genezzo::GenDBI->new(exe => $0, 
                         gnz_home => $glob_gnz_home, 
                         dbinit => $glob_init,
                         defs   => $glob_defs
                         );

unless (defined($fb))
{
    my $initmsg = 
        "use \n\t$0 -init \n\nto create a default installation.\n";

    if ($glob_init)
    {
        $initmsg = 
        "use \n\t$0 -define force_init_db=1 \n\n" .
        "to overwrite (and destroy) an existing installation.\n"
    }

    pod2usage(-exitstatus => 2, -verbose => 0, 
              -msg => $glob_id . $initmsg
              );
    # Note: exit takes zero for success, 1 for failure
    exit (1);
}

exit(0) # no interactive
    if ($glob_shutdown);

exit(!$fb->Interactive()); # invert status code for exit
