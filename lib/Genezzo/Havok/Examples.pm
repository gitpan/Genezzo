#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/Havok/RCS/Examples.pm,v 7.1 2005/07/19 07:49:03 claude Exp claude $
#
# copyright (c) 2005 Jeffrey I Cohen, all rights reserved, worldwide
#
#
package Genezzo::Havok::Examples;
require Exporter;

@ISA = qw(Exporter);
@EXPORT = qw(&isRedGreen);

use Genezzo::Util;

use strict;
use warnings;

use Carp;

# select * from mytable where Genezzo::Havok::Examples::isRedGreen(col1)
# select * from mytable where isRedGreen(col1)

# Example for UserExtend function: test if a string matches 
# the regexp "red or green"
sub isRedGreen
{
    return undef
        unless scalar(@_);

    return ($_[0] =~ m/^(red|green)$/i);
}

# Example for SysHook function
our $Howdy_Hook;
sub Howdy
{
    my %args = @_;

    if (exists($args{self}))
    {
        my $self = $args{self};
        if (defined($self) && exists($self->{GZERR}))
        {
            my $err_cb = $self->{GZERR};
            &$err_cb(self => $self, severity => 'info',
                     msg => "Howdy!!");
        }
    }

   # call the callback
    {
        if (defined($Howdy_Hook))
        {
            my $foo = $Howdy_Hook;
            return 0
                unless (&$foo(self => $args{self}));
        }
    }

    return 1;
}

our $Ciao_Hook;
sub Ciao
{
    my %args = @_;

    if (exists($args{self}))
    {
        my $self = $args{self};
        if (defined($self) && exists($self->{GZERR}))
        {
            my $err_cb = $self->{GZERR};
            &$err_cb(self => $self, severity => 'info',
                     msg => "Ciao!!");
        }
    }

    # call the callback
    {
        if (defined($Ciao_Hook))
        {
            my $foo = $Ciao_Hook;

            return 0
                unless (&$foo(self => $args{self}));
        }
    }

    return 1;
}

END { }       # module clean-up code here (global destructor)

## YOUR CODE GOES HERE

1;  # don't forget to return a true value from the file

__END__
# Below is stub documentation for your module. You better edit it!

=head1 NAME

Genezzo::Havok::Examples - some example havok functions

=head1 SYNOPSIS

=head1 DESCRIPTION

Havok test module 

=head1 ARGUMENTS

=head1 FUNCTIONS

=over 4

=item  isRedGreen - test if argument is red or green

=back

=head2 EXPORT

=over 4


=back


=head1 LIMITATIONS


=head1 AUTHOR

Jeffrey I. Cohen, jcohen@genezzo.com

=head1 SEE ALSO

L<perl(1)>.

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
