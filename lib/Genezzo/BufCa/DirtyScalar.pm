#!/usr/bin/perl
#
# $Header: /Users/claude/g3/lib/Genezzo/BufCa/RCS/DirtyScalar.pm,v 6.1 2004/08/12 09:31:15 claude Exp claude $
#
# copyright (c) 2003, 2004 Jeffrey I Cohen, all rights reserved, worldwide
#
#
use strict;
use warnings;

package Genezzo::BufCa::DirtyScalar;

use Genezzo::Util;
use Tie::Scalar;
#use Carp;
#use warnings::register;

our @ISA = qw(Tie::StdScalar) ;

sub TIESCALAR {
    my $class = shift;
    my $instance = shift || undef;
    # XXX: could use an array for efficiency
    # [0] for the ref, [1] for the callback
    my $self = {};
    $self->{ref} = \$instance;

    return bless $self, $class;
}

sub FETCH {
#    whoami;
    return ${$_[0]->{ref}};
}

sub STORE {
#    whoami;
    
    if (defined($_[0]->{storecb}))
    {
        $_[0]->{storecb}->();
    }
    ${$_[0]->{ref}} = $_[1];
}

sub DESTROY {
#    whoami;
    $_[0]->{ref} = ();
}

# supply a callback/closure to activate for during STORE
sub _StoreCB
{
#    whoami;
    my $self = shift;
    $self->{storecb} = shift if @_ ;
    return $self->{storecb};
}

1;

__END__

# Below is stub documentation for your module. You better edit it!

=head1 NAME

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 ARGUMENTS

=head1 FUNCTIONS

=head2 EXPORT

=head1 LIMITATIONS

various

=head1 #TODO

=over 4

=back

=head1 AUTHOR

Jeffrey I. Cohen, jcohen@genezzo.com

=head1 SEE ALSO

L<perl(1)>.

Copyright (c) 2003, 2004 Jeffrey I Cohen.  All rights reserved.

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
