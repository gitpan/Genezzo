#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/PushHash/RCS/HPHRowBlk.pm,v 6.3 2004/12/14 07:41:38 claude Exp claude $
#
# copyright (c) 2003, 2004 Jeffrey I Cohen, all rights reserved, worldwide
#
#
use strict;
use warnings;

package Genezzo::PushHash::HPHRowBlk;

use Genezzo::Util;
use Genezzo::PushHash::hph;
use Genezzo::PushHash::PushHash;
use Genezzo::PushHash::PHArray;
use Carp;
use warnings::register;

our @ISA = qw(Genezzo::PushHash::hph) ;

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

# private
sub _init
{
    #whoami;
    #greet @_;
    my $self = shift;
    my %args = (@_);

    return 1;
}

sub TIEHASH
{ #sub new 
#    greet @_;
    my $invocant = shift;
    my $class = ref($invocant) || $invocant ; 
    my $self = $class->SUPER::TIEHASH(@_);

    my %args = (@_);
    return undef
        unless (_init($self,%args));

    return bless $self, $class;

} # end new

# NOTE: block routine for index operations
sub _make_new_block
{
    my $self = shift;

#    whoami;

    # try to create a new block in the latest chunk (file)
    my $chunk = $self->_get_current_chunk();

    unless (defined($chunk))
    {
        my %earg = (self => $self, msg => "no chunk to make new block!");

        &$GZERR(%earg)
            if (defined($GZERR));

        return undef;
    }

    my $blockno;
    for my $i (1..2)
    {
        return undef
            unless (defined($chunk));

        $blockno = $chunk->_make_new_block();
#        greet $blockno;

        last  
            if (defined($blockno));

        # only one try if could not make a new block in new chunk
        return undef
            if ($i > 1);

        $chunk = $self->_make_new_chunk();
    }

    my $chunkno = $self->_currchunkno();

    return $self->_joinrid($chunkno, $blockno);
}

# NOTE: block routine for index operations and row splitting
sub _get_current_block
{
    my $self = shift;

#    whoami;

    my $chunk = $self->_get_current_chunk();

    unless (defined($chunk))
    {
        my %earg = (self => $self, msg => "no chunk to get block!");

        &$GZERR(%earg)
            if (defined($GZERR));

        return undef;
    }

    my $blockno = $chunk->_get_current_block();

    return $self->_make_new_block()
        unless (defined($blockno));

    my $chunkno = $self->_currchunkno();

    return $self->_joinrid($chunkno, $blockno);
} # _get_current_block

# NOTE: block routine for index operations
sub _get_block_and_bce
{
    my ($self, $place) = @_;

    my ($chunk, $sliceno) = $self->_get_chunk_and_slice($place);

    return undef
        unless (defined($chunk));

    return $chunk->_get_block_and_bce($sliceno);
}

# an augmented fetch.  FETCH returns a scalar, 
# but fetch2 can return an array
sub _fetch2
{
#    whoami;
    my $self  = shift;
    my $place = shift;
    my ($chunk, $sliceno)  = $self->_get_chunk_and_slice($place);

    unless (defined($chunk))
    {
        my %earg = (self => $self, msg => "No such key: $place ");

        &$GZERR(%earg)
            if (defined($GZERR));

        return undef;
    }

    #whoami $chunkno, $sliceno;
    return ($chunk->_fetch2($sliceno, @_));
} # end fetch2

# return existance and row status info.  Reset the row status if
# to newrowstat if it is set (use @_ for extra args)
sub _exists2
{
#    whoami;
    my $self  = shift;
    my $place = shift;
    my ($chunk, $sliceno)  = $self->_get_chunk_and_slice($place);

    unless (defined($chunk))
    {
        my %earg = (self => $self, msg => "No such key: $place ");

        &$GZERR(%earg)
            if (defined($GZERR));

        return undef;
    }

    #whoami $chunkno, $sliceno;
    return ($chunk->_exists2($sliceno, @_));
}

END {

}

1;

__END__
# Below is stub documentation for your module. You better edit it!

=head1 NAME

Genezzo::PushHash::HPHRowBlk.pm - a 90% pure virtual class module that
extends hierarchical "push hash" L<Genezzo::PushHash::hph> with Row/Block
methods.  These methods facilitate the construction of classes that
manipulate data blocks directly, such as index access methods and
functions that split rows over multiple blocks..

=head1 SYNOPSIS

 use Genezzo::PushHash::hph;

 sub make_fac {
    my $tclass = shift;
    my %args = (
                @_);

    my %td_hash1  = ();

    my $newfunc = 
        sub {
            my $tiehash1 = 
                tie %td_hash1, $tclass, %args;

            return $tiehash1;
        };
    return $newfunc;
 }

 my $fac1 = make_fac('Genezzo::PushHash::PHFixed');

 %args = 
    (
     factory  => $fac1
    );

 my %tied_hash = ();

 my $tie_val = 
    tie %tied_hash, 'Genezzo::PushHash::hph', %args;

 my $newkey = $tie_val->HPush("this is a test");

 $tied_hash{$newkey} = "update this entry";

 my $getcount = $tie_val->HCount();

=head1 DESCRIPTION

Like a standard hierarchical pushhash (hph), the HPHRowBlk is a
pushhash built upon a collection of other pushhashes.  A push into the
top-level hash is routed into one of the bottom hashes.  If the bottom
hashes are full (push fails), the top-level pushhash uses the factory
method to create or obtain a new pushhash.  The HPHRowBlk class is
designed to layer on top of hph's built of hash-tied byte block
storage, such as L<Genezzo::Row::RSBlock>.

=head1 CONCEPTS and INTERNALS - useful for implementors

A hph is constructed of N pushhash "chunks", and the elements of each
chunk are referred to as "slices".  Typically, one chunk is "current"
-- we push into the current chunk until it fills up, at which point
the hph attempts to make a new one.  HPHRowBlk is designed to expose
the underlying block mechanism to the uppermost layer of the pushhash.
It provides some additional methods: _make_new_block,
_get_current_block, and _get_block_and_bce, which provide
functionality somewhat similar to _get_current_chunk/_make_new_chunk,
but on a block level, versus individual scalar (packed row)
operations.  In addition, these methods "short-circuit" the hph tree
of pushhashes, making the bottom block operations directly available
to the top hph layer.  The penultimate layer of the hph stack (see
L<Genezzo::Row::RSFile>) must implement the internal block access methods
on the bottom pushhash.

=over 4

=item _make_new_block

create a new block in the current chunk
and return the block number as a rid.

=item _get_current_block

return the block number of the insertion position in the
current chunk.

=item _get_block_and_bce

return an array of the tied block, the buffer cache element
(see L<Genezzo::BufCa::BufCaElt>), and other useful information.

=back


=head1 WHY?

=over 4

=item Indexes

Btree indexes are implemented as a tree of data blocks.  Tree
operations directly manipulate the blocks directly, bypassing the hph
mechanisms that typically isolate the persistent tuple storage from
the top layer.  See L<Genezzo::Index::bt3>.

=item Row/Column Splitting

When a packed tuple exceeds the size of an individual block, the row
may be split over multiple blocks.  The basic semantics of the row
contents is only understood at the uppermost layer, which packs and
interprets tuple data, while the bottommost layer is solely
responsible for storing and accessing scalar byte string data in
persistent storage.  The HPHRowBlk methods provide handles into the
basic block storage so the upper layer can split and reconstruct row
data over multiple blocks.  See L<Genezzo::Row::RSTab>.

=back

=head1 AUTHOR

Jeffrey I. Cohen, jcohen@genezzo.com

=head1 SEE ALSO

L<Genezzo::PushHash::hph>,
L<Genezzo::PushHash::PushHash>,
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

For more information, please visit the Genezzo homepage 
at http://www.genezzo.com

=cut
