#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/Havok/UserExtend.pm,v 1.2 2004/09/27 08:45:05 claude Exp claude $
#
# copyright (c) 2004 Jeffrey I Cohen, all rights reserved, worldwide
#
#
package Genezzo::Havok::UserExtend;
use Genezzo::Util;

use strict;
use warnings;
use warnings::register;

use Carp;

sub HavokInit
{
#    whoami;
    my %optional = (phase => "init");
    my %required = (dict  => "no dictionary!"
                    );

    my %args = (%optional,
		@_);
#		

#    whoami (%args);

    return 0
        unless (Validate(\%args, \%required));

    my $dict   = $args{dict};
    my $phase  = $args{phase};

    return 0
        unless ($dict->DictTableExists(tname => "UserExtend",
                                       silent_notexists => 1));

    my $hashi  = $dict->DictTableGetTable (tname => "UserExtend") ;

    return 0 # no User Extensions
        unless (defined ($hashi));

    my $tv = tied(%{$hashi});

    while ( my ($kk, $vv) = each ( %{$hashi}))
    {
        my $getcol  = $dict->_get_col_hash("UserExtend");  
        my $xid     = $vv->[$getcol->{xid}];
        my $xtype   = $vv->[$getcol->{xtype}];
        my $xname    = $vv->[$getcol->{xname}];
        my $owner   = $vv->[$getcol->{owner}];
        my $dat     = $vv->[$getcol->{creationdate}];
        my $xargs  = $vv->[$getcol->{args}];

#        greet $vv;

        if ($xtype =~ m/^require$/i)
        {
            whisper "require $xname";
            unless (eval "require $xname")
            {
                carp "no such package - $xname"
                    if warnings::enabled();
                next;
            }
            no strict 'refs';
            unless (1) ### XXX: (eval "import $xname $xargs")
            {
                carp "bad import of $xargs for  package - $xname"
                    if warnings::enabled();
                next;
            }
            
        }
        elsif ($xtype =~ m/^function$/i)
        {
            my $doublecolon = "::";

            unless ($xname =~ m/$doublecolon/)
            {
                # Note: add functions to "main" namespace...

                $xname = "Genezzo::GenDBI::" . $xname;
            }

            my $func = "sub " . $xname . " " . $xargs;
            
#            whisper $func;

#            eval {$func } ;
            eval " $func " ;
            if ($@)
            {
                whisper "bad function : $func";
            }
        }
        else
        {
            carp "unknown phase - $phase"
                if warnings::enabled();

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

Genezzo::Havok::UserExtend

=head1 SYNOPSIS

Basic Havok module - load the UserExtend table

=head1 DESCRIPTION

=head1 ARGUMENTS

=head1 FUNCTIONS

=over 4

=item  isRedGreen

=back

=head2 EXPORT

=over 4


=back


=head1 LIMITATIONS

=head1 TODO

=over 4

=item Need to fix "import" mechanism so can load specific functions
into Genezzo::GenDBI namespace.

=back



=head1 AUTHOR

Jeffrey I. Cohen, jcohen@genezzo.com

=head1 SEE ALSO

L<perl(1)>.

Copyright (c) 2004 Jeffrey I Cohen.  All rights reserved.

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
