#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/Havok/RCS/RedGreen.pm,v 1.4 2004/12/14 07:48:17 claude Exp claude $
#
# copyright (c) 2004 Jeffrey I Cohen, all rights reserved, worldwide
#
#
package Genezzo::Havok::RedGreen;
require Exporter;

@ISA = qw(Exporter);
@EXPORT = qw(&isRedGreen);

use Genezzo::Util;

use strict;
use warnings;

use Carp;

# select * from mytable where Genezzo::Havok::RedGreen::isRedGreen(col1)
# select * from mytable where isRedGreen(col1)

sub isRedGreen
{
    return undef
        unless scalar(@_);

    return ($_[0] =~ m/^(red|green)$/i);
}

END { }       # module clean-up code here (global destructor)

## YOUR CODE GOES HERE

1;  # don't forget to return a true value from the file

__END__
# Below is stub documentation for your module. You better edit it!

=head1 NAME

Genezzo::Havok::RedGreen - test if argument is red or green

=head1 SYNOPSIS

=head1 DESCRIPTION

Havok test module 

=head1 ARGUMENTS

=head1 FUNCTIONS

=over 4

=item  isRedGreen

=back

=head2 EXPORT

=over 4


=back


=head1 LIMITATIONS


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

For more information, please visit the Genezzo homepage 
at http://www.genezzo.com

=cut
