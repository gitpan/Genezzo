#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/RCS/Havok.pm,v 1.2 2004/09/27 08:44:12 claude Exp claude $
#
# copyright (c) 2003, 2004 Jeffrey I Cohen, all rights reserved, worldwide
#
#
package Genezzo::Havok;
use Genezzo::Util;
use Genezzo::Dict;

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

#        greet $vv;

        unless (eval "require $modname")
        {
            carp "no such package - $modname"
                if warnings::enabled();
            next;
        }

        my $stat;
        if ($phase =~ m/^(init|cleanup)$/i)
        {
            my $p2   = ucfirst($phase);
            my $func = $modname . "::" . "Havok" . $p2;
            no strict 'refs' ;
            eval {$stat = &$func(%args) };
            if ($@)
            {
                whisper "bad lc($phase) : $modname";
            }
            whisper "bad return status : $func"
                unless ($stat);
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

Genezzo::Havok - Cry Havok! And Let Slip the Dogs of War!

=head1 SYNOPSIS

use Genezzo::Havok; # Wreak Havok

=head1 DESCRIPTION

After database startup, the Havok subsystem runs arbitrary
code to modify your Genezzo installation.  

=head1 ARGUMENTS

=head1 FUNCTIONS

=over 4

=item  HavokInit

=item  HavokCleanup

=back

=head2 EXPORT

=over 4


=back


=head1 LIMITATIONS



=head1 TODO

=over 4

=item  Create dictionary initialization havok

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
