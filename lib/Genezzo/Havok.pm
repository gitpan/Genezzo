#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/RCS/Havok.pm,v 1.9 2004/12/30 07:12:59 claude Exp claude $
#
# copyright (c) 2003,2004,2005 Jeffrey I Cohen, all rights reserved, worldwide
#
#
package Genezzo::Havok;
use Genezzo::Util;
use Genezzo::Dict;

use strict;
use warnings;
use warnings::register;

use Carp;

our $VERSION;

BEGIN {
    $VERSION = do { my @r = (q$Revision: 1.9 $ =~ /\d+/g); sprintf "%d."."%02d" x $#r, @r }; # must be all one line, for MakeMaker

}

our $GZERR = sub {
    my %args = (@_);

    return 
        unless (exists($args{msg}));

    if (exists($args{self}))
    {
        my $self = $args{self};
        if (defined($self) && exists($self->{GZERR}))
        {
            my $err_cb = $self->{GZERR};
            return &$err_cb(%args);
        }
    }

    carp $args{msg}
        if warnings::enabled();
    
};

sub Install
{


}

sub MakeSQL
{
    my $bigSQL; 
    ($bigSQL = <<EOF_SQL) =~ s/^\#//gm;
#REM Copyright (c) 2004, 2005 Jeffrey I Cohen.  All rights reserved.
#REM
#REM 
#ct havok hid=n modname=c owner=c creationdate=c flag=c version=c
#ct user_extend xid=n xtype=c xname=c args=c owner=c creationdate=c version=c
#i havok 1 Genezzo::Havok SYSTEM TODAY 0 HAVOK_VERSION
#i havok 2 Genezzo::Havok::UserExtend SYSTEM TODAY 0 0
#
#REM 
#REM select * from tab1 where Genezzo::Havok::RedGreen::isRedGreen(col1)
#i user_extend 1 require Genezzo::Havok::RedGreen isRedGreen SYSTEM TODAY 0
#i user_extend 2 require Text::Soundex soundex SYSTEM TODAY 0
#
#
#
#commit
#shutdown
#startup
EOF_SQL
    my $now = Genezzo::Dict::time_iso8601();
    $bigSQL =~ s/TODAY/$now/gm;
    $bigSQL =~ s/HAVOK_VERSION/$VERSION/gm;
    $bigSQL = "REM Generated by " . __PACKAGE__ . " version " .
        $VERSION . " on $now\nREM\n" . $bigSQL;

#    print $bigSQL;

#REM select * from tab1 where isBlueYellow(col1)
#i user_extend 3 function isBlueYellow '{return undef unless scalar(@_);   return ($_[0] =~ m/^(blue|yellow)$/i); }' SYSTEM TODAY

    return $bigSQL;
}

sub HavokInit
{
#    whoami;
    my %optional = (phase => "init");
    my %required = (dict  => "no dictionary!",
                    flag  => "no flag"
                    );

    my %args = (%optional,
		@_);
#		

#    whoami (%args);

    return 0
        unless (Validate(\%args, \%required));

    my $dict   = $args{dict};
    my $phase  = $args{phase};

    return 1
        unless ($dict->DictTableExists(tname => "havok",
                                       silent_notexists => 1));

    my $hashi  = $dict->DictTableGetTable (tname => "havok") ;

    return 1 # no havok table
        unless (defined ($hashi));

    my $tv = tied(%{$hashi});

    while ( my ($kk, $vv) = each ( %{$hashi}))
    {
        my $getcol  = $dict->_get_col_hash("havok");  
        my $hid     = $vv->[$getcol->{hid}];
        my $modname = $vv->[$getcol->{modname}];
        my $owner   = $vv->[$getcol->{owner}];
        my $dat     = $vv->[$getcol->{creationdate}];
        my $flag    = $vv->[$getcol->{flag}];
        my $verzion = $vv->[$getcol->{version}];

#        greet $vv;

        # check if have right version of this package
        if ($modname eq "Genezzo::Havok")
        {
            unless ($VERSION eq $verzion)
            {
                # XXX XXX: do something
                my $msg = "$modname version mismatch - " .
                    "current version $VERSION " . 
                    "!= $verzion in database table";

                my %earg = (#self => $self,
                            severity => 'warn',
                            msg => $msg);

                &$GZERR(%earg)
                    if (defined($GZERR));
            }
            next;
        }

        unless (eval "require $modname")
        {
            my %earg = (#self => $self,
                        severity => 'warn',
                        msg => "no such package - $modname");

            &$GZERR(%earg)
                if (defined($GZERR));

            next;
        }

        my %nargs;
        $nargs{dict} = $dict;
        $nargs{flag} = $flag;
        $nargs{version} = $verzion;

        my @stat;
        if ($phase =~ m/^(init|cleanup)$/i)
        {
            my $p2   = ucfirst($phase);
            my $func = $modname . "::" . "Havok" . $p2;
            no strict 'refs' ;
            eval {@stat = &$func(%nargs) };
            if ($@)
            {
                my %earg = (#self => $self,
                            severity => 'warn',
                            msg => "$@\n" .
                            "bad " . lc($phase) . " : $modname");

                &$GZERR(%earg)
                    if (defined($GZERR));
            }
            unless ($stat[0])
            {
                my %earg = (#self => $self,
                            severity => 'warn',
                            msg => "bad return status : $func");

                &$GZERR(%earg)
                    if (defined($GZERR));
            }
        }
        else
        {
            my %earg = (#self => $self,
                        severity => 'warn',
                        msg => "unknown phase - $phase");

            &$GZERR(%earg)
                if (defined($GZERR));
        }

    } # end while

    return 1;
}

sub HavokCleanup
{
#    whoami;
    return HavokInit(@_, phase => "cleanup");
}


END { }       # module clean-up code here (global destructor)

## YOUR CODE GOES HERE

1;  # don't forget to return a true value from the file

__END__
# Below is stub documentation for your module. You better edit it!

=head1 NAME

Genezzo::Havok - Cry Havok! And Let Slip the Dogs of War!

=head1 SYNOPSIS

use Genezzo::Havok; # Wreak Havok

create table havok (
    hid          number,
    modname      char,
    owner        char,
    creationdate char, 
    flag         char,
    version      char
);

=over 4

=item  hid - a unique id number

=item  modname - a havok module name

=item  owner - module owner

=item  creationdate - date of row creation

=item  flag - (user-defined)

=item  version - module version information

=back

=head1 DESCRIPTION

After database startup, the Havok subsystem runs arbitrary
code to modify your Genezzo installation.  

=head2 WHY?

Havok lets you construct novel, sophisticated extensions to Genezzo as
"plug-ins".  The basic Genezzo database kernel can remain small, and
users can download and install additional packages to extend Genezzo's
functionality.  This system also gives you a modular upgrade capability.

=head2 Examples

See L<Genezzo::Havok::UserExtend>, a module that lets users install
custom functions or entire packages.  The Havok regression test,
B<t/Havok1.t>, loads L<Text::Soundex> and demonstrates a soundex
comparison of strings in a table.  You can easily add other string or
mathematical functions.


=head1 ARGUMENTS

=head1 FUNCTIONS

=over 4

=item  HavokInit
Calls the HavokInit for every module in the havok table, supplying a
hash of the dictionary, the flag, and the module version.  The clients
should return an array where the first element is a success code (0 or 1)
and the second is the updated flag (if necessary).

=item  HavokCleanup

=back

=head2 EXPORT

=over 4


=back


=head1 LIMITATIONS

Havok is intended for specialized packages which extend the
fundamental database mechanisms.  If you only want to add new SQL
functions, then you should use L<Genezzo::Havok::UserExtend>.


=head1 TODO

=over 4

=item  Create dictionary initialization havok (vs post-startup havok)

=item  Need some type of first-time registration function.  For
example, if your extension module needs to install new dictionary
tables.  Probably can add arg to havokinit, and add a flag to havok
table to track init status.

=item  Safety/Security: could load modules using Safe package to
restrict their access (not a perfect solution).  May also want to
construct a dictionary wrapper to restrict dictionary capabilities for
certain clients, e.g. let a package read, but not update, certain
dictionary tables.

=item  Force Init/ReInit when new package is loaded.

=item  update module flags if necessary, handle cleanup

=back

=head1 AUTHOR

Jeffrey I. Cohen, jcohen@genezzo.com

=head1 SEE ALSO

L<perl(1)>.

Copyright (c) 2004, 2005 Jeffrey I Cohen.  All rights reserved.

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

For more information, please visit the Genezzo homepage 
at L<http://www.genezzo.com>

=cut
