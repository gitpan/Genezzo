#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/RCS/genexp.pl,v 7.1 2005/07/19 07:49:03 claude Exp claude $
#
# copyright (c) 2005 Jeffrey I Cohen, all rights reserved, worldwide
#
#
#use strict;
use Genezzo::GenDBI;
use Data::Dumper;
use Getopt::Long;
use Pod::Usage;
use strict;
use warnings;

=head1 NAME

B<genexp.pl> -  Genezzo database exporter

=head1 SYNOPSIS

B<genexp> [options] 

Options:

    -help            brief help message
    -man             full documentation
    -gnz_home        supply a directory for the gnz_home
    -define key=val  define a configuration parameter

=head1 OPTIONS

=over 8

=item B<-help>

    Print a brief help message and exits.

=item B<-man>
    
    Prints the manual page and exits.

=item B<-gnz_home>
    
    Supply the location for the gnz_home installation.  If 
    specified, it overrides the GNZ_HOME environment variable.


=item B<-define> key=value
    
    If initializing a new database, define a configuration 
    parameter.

=back

=head1 DESCRIPTION

Genezzo is an extensible, persistent datastore that uses a subset of
SQL.  The genexp tool lets users export their existing schema as a SQL 
script.  Running the script will recreate and repopulate the tables.

=head2 Environment

GNZ_HOME: If the user does not specify a gnz_home directory using 
the B<'-gnz_home'> option, Genezzo stores dictionary and table
information in the location specified by this variable.  If 
GNZ_HOME is undefined, the default location is $HOME/gnz_home.

=head1 AUTHORS

Copyright (c) 2005 Jeffrey I Cohen.  All rights reserved.  

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
    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

Address bug reports and comments to: jcohen@genezzo.com

For more information, please visit the Genezzo homepage 
at L<http://www.genezzo.com>

=cut

our $GZERR = sub {
    my %args = (@_);

    return 
        unless (exists($args{msg}));

    my $warn = 0;
    if (exists($args{severity}))
    {
        my $sev = uc($args{severity});
        $sev = 'WARNING'
            if ($sev =~ m/warn/i);

        # don't print 'INFO' prefix
        if ($args{severity} !~ m/info/i)
        {
            printf ("%s: ", $sev);
            $warn = 1;
        }

    }
    print $args{msg};
    # add a newline if necessary
    print "\n" unless $args{msg}=~/\n$/;
#    carp $args{msg}
#      if (warnings::enabled() && $warn);
    
};


    my $glob_gnz_home;
    my $glob_id;
    my $glob_defs;

sub setinit
{
    $glob_gnz_home = shift;
    $glob_defs     = shift;
}

BEGIN {
    my $man  = 0;
    my $help = 0;
    my $gnz_home = '';
    my %defs = ();      # list of --define key=value

    GetOptions(
               'help|?' => \$help, man => \$man, 
               'gnz_home=s' => \$gnz_home,
               'define=s'   => \%defs)
        or pod2usage(2);

    $glob_id = "Genezzo Version $Genezzo::GenDBI::VERSION - $Genezzo::GenDBI::RELSTATUS $Genezzo::GenDBI::RELDATE\n\n"; 

    pod2usage(-msg => $glob_id, -exitstatus => 1) if $help;
    pod2usage(-msg => $glob_id, -exitstatus => 0, -verbose => 2) if $man;

    setinit($gnz_home,  \%defs);

#    print "loading...\n" ;
}

my $dbh = Genezzo::GenDBI->new(exe => $0, 
                              gnz_home => $glob_gnz_home, 
                              defs   => $glob_defs,
                              GZERR  => $GZERR
                         );

unless (defined($dbh))
{
    my $initmsg = 
        "use \n\t$0 -init \n\nto create a default installation.\n";


    pod2usage(-exitstatus => 2, -verbose => 0, 
              -msg => $glob_id . $initmsg
              );
    # Note: exit takes zero for success, 1 for failure
    exit (1);
}

my $stat = 0;

{
    unless($dbh->do("startup"))
    {
        $stat = 1;
        last;
    }


    # tid = 10 for last dict table
    my $sth = 
        $dbh->prepare("select tid, tname from _tab1 where tid > 10 and object_type='TABLE'");

#    print $sth->execute(), " rows \n";
    $sth->execute();

    my @tabs;
    while (1)
    {
        my @ggg = $sth->fetchrow_array();
        
#    print Dumper (@ggg), "\n";
        
        last
            unless (scalar(@ggg));
        
        push @tabs, [@ggg];

    }

#    print Dumper(@tabs), "\n";
# get all tables with tid > cons1_cols

    for my $tabi (@tabs)
    {
        my $tid = $tabi->[0];

        my $sql = 
            "select colidx, colname, coltype, tid, tname from _col1 where tid = $tid";
        $sth = 
            $dbh->prepare($sql);

#    print $sth->execute(), " rows \n";
        $sth->execute();

        my @cols = ();

        while (1)
        {
            my @ggg = $sth->fetchrow_array();
        
#            print Dumper (@ggg), "\n";
        
            last
                unless (scalar(@ggg));
            
            my $colidx = shift @ggg;

            $cols[$colidx] = [@ggg];
        }

        print "ct $tabi->[1] ";

        for my $coli (@cols)
        {
            print $coli->[0],"=",$coli->[1]," "
                if (defined($coli));
        }
        print "\n";

    }

    for my $tabi (@tabs)
    {
        my $tname = $tabi->[1];

        my $sql = 
            "select * from $tname ";
        $sth = 
            $dbh->prepare($sql);

#    print $sth->execute(), " rows \n";
        $sth->execute();

        my @cols = ();

        while (1)
        {
            local $Data::Dumper::Terse = 1;

            my $firsttime;
            my @fff = $sth->fetchrow_array();
            my @ggg = Dumper(@fff);
#            my @ggg = @fff;

#            print Dumper (@ggg), "\n";
        
            last
                unless (scalar(@ggg));
            
            print "insert into $tname values(";

            $firsttime = 1;
            for my $colcnt (1..scalar(@fff))
            {
                print ", "
                    unless $firsttime;
                if (defined($fff[$colcnt-1]))
                {
                    my $outi = $ggg[$colcnt-1];
                    $outi =~ s/\n$//;
#                    print  "'",$outi,"'";
                    print $outi;
                }
                else
                {
                    print "NULL";
                }

                $firsttime = 0
            }
            print ");\n";

        }

    }

}

exit($stat) 

