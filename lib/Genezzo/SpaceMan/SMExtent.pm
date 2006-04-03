#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/SpaceMan/RCS/SMExtent.pm,v 1.16 2006/03/14 08:20:39 claude Exp claude $
#
# copyright (c) 2006 Jeffrey I Cohen, all rights reserved, worldwide
#
#
package Genezzo::SpaceMan::SMExtent;

use strict;
use warnings;

use Carp;
use Genezzo::Util;
use Genezzo::Row::RSBlock;
use Genezzo::SpaceMan::SMFile;

BEGIN {
    use Exporter   ();
    our ($VERSION, @ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS);

    # set the version for version checking
#    $VERSION     = 1.00;
    # if using RCS/CVS, this may be preferred
    $VERSION = do { my @r = (q$Revision: 1.16 $ =~ /\d+/g); sprintf "%d."."%02d" x $#r, @r }; # must be all one line, for MakeMaker

    @ISA         = qw(Exporter);
    @EXPORT      = ( ); # qw(&NumVal);
    %EXPORT_TAGS = ( );     # eg: TAG => [ qw!name1 name2! ],

    # your exported package globals go here,
    # as well as any optionally exported functions
#    @EXPORT_OK   = qw($Var1 %Hashit &func3 &func5);
    @EXPORT_OK   = (); 

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
    # XXX XXX XXX
    print __PACKAGE__, ": ",  $args{msg};
#    print $args{msg};
#    carp $args{msg}
#      if (warnings::enabled() && $warn);
    
};


sub _init
{
    #whoami;
    #greet @_;
    my $self      =  shift;
    my %required  =  (
                      filename   => "no filename !",
                      numbytes   => "no numbytes !",
                      numblocks  => "no numblocks !",
                      bufcache   => "no bufcache !",
                      filenumber => "no filenumber !",
                      tablename  => "no tablename !",
                      object_id  => "no object id !"
                      );
    
    my %args = (@_);

    return undef
        unless (Validate(\%args, \%required));

    my $smf = Genezzo::SpaceMan::SMFile->new($args{filename},
                                             $args{numbytes},
                                             $args{numblocks},
                                             $args{bufcache},
                                             $args{filenumber});

    return undef
        unless (defined($smf));

    $self->{smf} = $smf;

    my %nargs   = (tablename  => $args{tablename},
                   object_id  => $args{object_id}
                   );
    
    my $blockno = $self->{smf}->firstblock(%nargs);

    if (defined($blockno))
    {
        $self->{first_extent}   = $blockno;
        $self->{current_seghdr} = $blockno;

        # need to call this way because SELF isn't BLESSed yet...
        my $rowd = _get_rowd($self, $blockno);
        unless (defined($rowd))
        {
            return (undef);
        }

        # get meta data for the segment header
        my $row  = $rowd->_get_meta_row("X1A");

# XXX XXX: remove seghdr 
        # if the current hdr is full...
        while (defined($row) && (scalar(@{$row}) > 1)
               && ($row->[1] =~ m/F/))
        {
            # overflow header is listed in x1b
            my $row2 = $rowd->_get_meta_row("X1B");

            # XXX XXX: need some error checking here...
            last
                unless (defined($row2) && (scalar(@{$row2} > 2)));

            my $nexthdr = $row2->[-1]; # check the end of the array

            my @ggg = split(':', $nexthdr);

            last 
                unless (scalar(@ggg) > 1);

            # advance to the next header...
            $blockno = $ggg[0];
            $self->{current_seghdr} = $blockno;

# XXX XXX            print "advance to next header, block $blockno\n";

            $rowd = _get_rowd($self, $blockno);
            unless (defined($rowd))
            {
                return (undef);
            }

            # get meta data for the segment header
            $row  = $rowd->_get_meta_row("X1A");

        } # end while
        

##        print Data::Dumper->Dump([$row]), "\n";
 
        # need to call this way because SELF isn't BLESSed yet...       
        $rowd = _get_rowd($self, 0);
        unless (defined($rowd))
        {
            return (undef);
        }
    }

    return 1;

}

sub new 
{
    my $invocant = shift;
    my $class = ref($invocant) || $invocant ; 
    my $self = { };
    
#    whoami @_;
    my %args = (@_);
    return undef
        unless (_init($self,%args));

    if ((exists($args{GZERR}))
        && (defined($args{GZERR}))
        && (length($args{GZERR})))
    {
        # NOTE: don't supply our GZERR here - will get
        # recursive failure...
        $self->{GZERR} = $args{GZERR};
    }

    my $blessref = bless $self, $class;

    return $blessref;

} # end new

sub _get_rowd # private
{
    my ($self, $blockno) = @_;

    $blockno = 0
        unless (defined($blockno));

    my $bc = $self->{smf}->{bc};

    unless ($self->{smf}->_tiefh($bc, $blockno))
    {
        whisper "bad fh tie";
        my $msg = "bad fh tie\n";
        my %earg = (self => $self, msg => $msg,
                    severity => 'warn');
        
        &$GZERR(%earg)
            if (defined($GZERR));

        return (undef);
    }
    my $fileheader = $bc->{fileheader};
    
    my $rowd = $fileheader->{RealTie};

    unless (defined($rowd))
    {
        whisper "bad fh real tie";
        my $msg = "bad fh real tie\n";
        my %earg = (self => $self, msg => $msg,
                    severity => 'warn');
        
        &$GZERR(%earg)
            if (defined($GZERR));

    }
    return $rowd;
}

# for Tablespace::TSGrowFile
sub _file_info
{
    my $self = shift;
    return $self->{smf}->_file_info(@_);
}

# for Tablespace::TSGrowFile
sub SMGrowFile
{
    my $self = shift;
    return $self->{smf}->SMGrowFile(@_);
}

sub _create_segment_hdr
{
    my ($self, $rowd, $blockno, $extent_size, $parent) = @_;

    return undef
        unless (defined($rowd));

    $rowd->_set_meta_row("X1A", 
                         [
# XXX XXX: remove seghdr
                          "seghdr", 
                          "V", # [V]acant, vs [F]ull
                          join(':', $blockno, $extent_size, '0.0')
                          ]
                         );

    unless (defined($parent))
    {
        # should only be for very first allocated extent
        $self->{first_extent}   = $blockno;
        $self->{current_seghdr} = $blockno;
        $parent = $blockno;
    }

    # no "next" segment header, just list parent piece of seg hdr
    $rowd->_set_meta_row("X1B", [
# XXX XXX: remove segnxt
                                 "segnxt", 
                                 $parent]);

    return $rowd;
} # end _create_segment_hdr

sub _update_segment_hdr
{
    my ($self, $blockno, $extent_size) = @_;

#    whisper "update seg hdr\n";

    my ($row, $row2, $rowd, $val);

  L_bigW:
    while (1)
    {
        $rowd = $self->_get_rowd($self->{current_seghdr});
        unless (defined($rowd))
        {
            return (undef);
        }
        # get meta data for the segment header
        $row2 = $rowd->_get_meta_row("X1B");

        # if current segment header does not have a child for overflow,
        # then the new extent is allocated for that purpose
        if (defined($row2) && (scalar(@{$row2}) < 3)) # XXX XXX: remove segnxt
        {
            push @{$row2}, join(':', $blockno, $extent_size, '0.0');

            $rowd->_set_meta_row("X1B", $row2);

            $rowd = $self->_get_rowd($blockno);
            unless (defined($rowd))
            {
                return (undef);
            }
            
            my $parent = $self->{current_seghdr};

            # create the new segment subheader in the new extent
            $self->_create_segment_hdr($rowd, 
                                       $blockno, $extent_size, 
                                       $parent);

            $rowd = $self->_get_rowd($self->{current_seghdr});
            unless (defined($rowd))
            {
                return (undef);
            }
        } # end if make overflow

        $row = $rowd->_get_meta_row("X1A");

        unless (defined($row))
        {
            croak "serious error 4!";
            exit;
        }

        my $new_ext = join(':', $blockno, $extent_size, '0.0');

        push @{$row}, $new_ext;

        if (0 && scalar(@{$row}) > 4)
        {
#            print "overflow to next header\n";
            $val = undef;
        }
        else
        {

            $val = $rowd->_set_meta_row("X1A", $row);
        }
        
        ########################################
        #  end here if set meta row correctly  #
        ########################################
        last L_bigW
            if (defined($val));

        # else we set the current header as full, and use the next
        pop @{$row};
# XXX XXX: remove seghdr 
        $row->[1] = 'F' ; # row is full
        $val = $rowd->_set_meta_row("X1A", $row);
        unless (defined($val))
        {
            # should have been able to just update status flag
            croak "serious error 1!";
            exit;
        }
        
        # find the overflow header
        $row2 = $rowd->_get_meta_row("X1B");
        unless (defined($row2) && (scalar(@{$row2} > 2)))
        {
            croak "serious error 2!";
            exit;
        }

        my $nexthdr = $row2->[-1]; # check the end of the array

        my @ggg = split(':', $nexthdr);

        unless (scalar(@ggg) > 1)
        {
            croak "serious error 3!";
            exit;
        }
        
        # find the blockno of the overflow header, and make it current
##        $parent = $self->{current_seghdr};
        $self->{current_seghdr} = $ggg[0];
        # try again...
    } # end while
    
    return $rowd;
} # end _update_segment_hdr

sub _create_extent_hdr
{
    my ($self, $rowd, $extent_size) = @_;

    return undef
        unless (defined($rowd));

    # XXX XXX: need enforcement of max extent size

    my $numbits = Genezzo::Util::PackBits(2 * $extent_size);
    my $nullstr = pack("B*", "0"x$numbits);

    $rowd->_set_meta_row("XHA", [$extent_size, $nullstr]);
    # extent position is zero (1st block in extent)
    $rowd->_set_meta_row("XHP", [0]);

    return $rowd;
} # end _create_extent_hdr

sub _update_extent_hdr
{
    my ($self, $rowd, $posn, $pct_used) = @_;

    return undef
        unless (defined($rowd));

    # get meta data for the segment header
    my $row = $rowd->_get_meta_row("XHA");

    if (! (defined($row) && scalar(@{$row})))
    {
        my $msg = "bad extent header\n";
        my %earg = (self => $self, msg => $msg,
                    severity => 'warn');
        
        &$GZERR(%earg)
            if (defined($GZERR));

        return undef;
    }

    my ($extent_size, $bvec) = @{$row};

    whisper "size, bv: $extent_size," , unpack("b*",$bvec), "\n";

    my ($bit1, $bit2) = (0, 0 ); # empty

    if ($pct_used > 99)
    {
        ($bit1, $bit2) = (1,1 ); # full
    }
    elsif ($pct_used >= 60)
    {
        ($bit1, $bit2) = (1,0 );
    }
    elsif ($pct_used >= 30)
    {
        ($bit1, $bit2) = (0,1 );
    }

    # use 2 bits per posn -- 00 is empty , 11 is full
    vec($bvec, (2*$posn),   1) = $bit1;
    vec($bvec, (2*$posn)+1, 1) = $bit2;
    whisper "size, bv: $extent_size," , unpack("b*",$bvec), "\n";
    # update
    $rowd->_set_meta_row("XHA", [$extent_size, $bvec]);

    return $rowd;
    
} # end _update_extent_hdr


sub _find_extent_hdr
{
    whoami;

#    print "find extent header \n";
    
    my ($self, $blockno) = @_;

#    whisper "update seg hdr\n";

    my $rowd = $self->_get_rowd($blockno);
    unless (defined($rowd))
    {
        return (undef);
    }
    # get meta data for the position
    my $row = $rowd->_get_meta_row("XHP");

    unless (defined($row))
    {
        my $msg = "no position!\n";
        my %earg = (self => $self, msg => $msg,
                    severity => 'warn');
        
        &$GZERR(%earg)
            if (defined($GZERR));

        return undef;
    }
    my $posn        = $row->[0];
    my $curr_extent = $blockno - $posn;

    whisper "curr ext: $curr_extent, pos: $posn \n";

    return ($curr_extent, $posn);
}

sub nextfreeblock
{
    # STUB: this routine for finding free blocks in allocated extents
    my $self = shift;
    # if no space available, get space from SMFile
    return $self->_file_nextfreeblock(@_);
}

sub _file_nextfreeblock
{
    my $self = shift;
    my $gotnewextent = 0; # true if get new extent
    my $blockinfo = $self->{smf}->nextfreeblock(@_);

    return undef
        unless (defined($blockinfo));

    {
        if (exists($blockinfo->{currextent}))
        {
            greet "new extent", $blockinfo ;
            $gotnewextent = 1; # true if get new extent
        }
    }

    my $blockno = $blockinfo->{basic_array}->[0];

    my $rowd = $self->_get_rowd($blockno);
    unless (defined($rowd))
    {
        return (undef);
    }

    my @new_extent_info;
    if (!$gotnewextent)
    {
        # check if already have extent position
        my $row = undef;

        # if the current extent isn't new, then curr_extent should
        # have been set when the extent was created.  However, if the
        # extent was created in a previous session, we need to track
        # down the header to determine the current position

        unless (exists($self->{curr_extent}) &&
                exists($self->{extent_posn}))
        { # no position info
            $row = $rowd->_get_meta_row("XHP");
            if (defined($row))
            {
                # if the "new" block has a position, then we are done

                whisper "found position!!\n";
                my $posn = $row->[0];
                $self->{curr_extent} = $blockno - $posn;
                $self->{extent_posn} = $posn;

                goto L_setposition;                
            }

            # if we don't know the start of the current extent, check
            # the previous block
            
            $rowd = undef;

            my @ggg = $self->_find_extent_hdr($blockno - 1);

            unless (scalar(@ggg) > 1)
            {
                my $msg = "could not find extent header!\n";
                my %earg = (self => $self, msg => $msg,
                            severity => 'warn');
        
                &$GZERR(%earg)
                    if (defined($GZERR));

                return undef;
            }
            # set info for previous block
            $self->{curr_extent} = shift @ggg;
            $self->{extent_posn} = shift @ggg;
            
            # reload the current block
            $rowd = $self->_get_rowd($blockno);
            unless (defined($rowd))
            {
                return (undef);
            }
 
        } # end no position info

        # if we know the blockno of the current extent, just
        # increment the extent position
        $self->{extent_posn} += 1;

      L_setposition:
        my $posn = $self->{extent_posn};
        # set meta data for the extent header
        $rowd->_set_meta_row("XHP", [$posn]);

        if (0) # XXX XXX: move this to a callback
        {
            # update the extent header
            $rowd = $self->_get_rowd($blockno - $posn);
            unless (defined($rowd))
            {
                return (undef);
            }
            my $old_posn = $self->{extent_posn} - 1;
            # update previous position as 100% used...
            unless ($self->_update_extent_hdr($rowd, $old_posn, 100))
            {
                my $msg = "could not update extent header\n";
                my %earg = (self => $self, msg => $msg,
                            severity => 'warn');
        
                &$GZERR(%earg)
                    if (defined($GZERR));

                return undef;
            }
        }

    }
    else # got new extent
    {
        # size of extent is last entry in blockinfo
        my $extent_size = $blockinfo->{currextent}->[-1];

        $self->{extent_size}  = $extent_size;

        # curr_extent is the block number of the start of the current extent
        $self->{curr_extent}  = $blockno;

        # each block in the extent has a "position" -- an offset from
        # the 1st block.  The 1st block is position zero, 
        # the 2nd is position 1, etc.
        $self->{extent_posn}  = 0;

        if ($blockinfo->{firstextent})
        {
            # set meta data for the segment header in this file
            unless ($self->_create_segment_hdr($rowd, $blockno, $extent_size))
            {
                my $msg = "could not create segment header\n";
                my %earg = (self => $self, msg => $msg,
                            severity => 'warn');
        
                &$GZERR(%earg)
                    if (defined($GZERR));

                return undef;
            }
        }
        else
        {
            # get info for segment header
            push @new_extent_info, $blockno, $extent_size;
        }
        # set meta data for the extent header
        unless ($self->_create_extent_hdr($rowd, $extent_size))
        {
            my $msg = "could not create extent header\n";
            my %earg = (self => $self, msg => $msg,
                        severity => 'warn');
        
            &$GZERR(%earg)
                if (defined($GZERR));

            return undef;
        }

    }

    if (scalar(@new_extent_info))
    {
        $self->_update_segment_hdr(@new_extent_info);
    }
        
    $rowd = $self->_get_rowd(0);
    unless (defined($rowd))
    {
        return (undef);
    }

    return $blockinfo;
} # end nextfreeblock

sub currblock
{
    my $self = shift;
    return $self->{smf}->currblock(@_);
}

sub firstblock
{
    my $self = shift;
    return $self->{smf}->firstblock(@_);
}

sub nextblock
{
    my $self = shift;
    return $self->{smf}->nextblock(@_);
}

sub countblock
{
    my $self = shift;
    return $self->{smf}->countblock(@_);
}

sub hasblock
{
    my $self = shift;
    return $self->{smf}->hasblock(@_);
}

sub freetable
{
    my $self = shift;
    return $self->{smf}->freetable(@_);
}

sub flush
{
    my $self = shift;
    return $self->{smf}->flush(@_);
}



END {

}


1;  # don't forget to return a true value from the file

__END__

=head1 NAME

Genezzo::SpaceMan::SMExtent.pm - Extent Space Management

=head1 SYNOPSIS


=head1 DESCRIPTION

Maintain segment headers and extent headers for objects stored in
a file with information on space usage.  The set of space allocations
for an object in a file is called a *segment*.  Each segment is
composed of *extents*, groups of contiguous blocks.

allocate a new extent:

if have first extent (segment header)
  create X1A with current blockno, size.
else 
  update X1A with new extent info.

in first block of extent:
  create XHA with empty space usage bitvec
  create XHP, marked as position zero 

if allocate a new block:
  if 1st block of an extent, 
    goto allocate new extent
  else
    could mark prior block as used in XHA...

if free a block:
  clear bitvec in XHA

if freed all blocks in XHA
  update X1A

if X1A is too small:
need 2 rows.  pump out with free space at end.

seghd_allextents: status_flag extent:size:pct_used, 
                  extent:size:pct_used, extent:size:pct_used...
seghd_next: parent_seghead next_seghead:tot_size:pct_used

tot_size in human_num, eg 10K, 100G, 2P...

leapfrog:

seghd in extent 1, create a seghd in extent 2 when
you allocate it.  
when seghd in extent 1 fills, overflow to seghd in 
extent 2.  When allocate next new extent, update the
2nd seghead, and create a new seghd in the new extent.

x1a: extent:size:pct_used, extent:size:pct_used, ...
x1b: parent [child]

parent = self for 1st extent
fill in child when allocate 2nd extent...
child info tracks additional space usage in segment subhead, and
if use "human readable" numbers, can restrict to 4 char fixed size
0-999B, 1K-999K, 1M-999M 
marker for "subhead full" vs vacancy...
x1a: full_flag extent:size:pct_used, extent:size:pct_used, ...


if XHA bitvec is too long:
break out over multiple rows, over multiple blocks.    

xhd1: parent_xhd bitvec
xhd[N]: next_xhd
  
or maybe -- recursive split 

bitvec of blocks or subextents.
for extent of < 128 blocks, simple bitvec for each block.

for extent of 256 blocks
top bitvec of 2 subextents
each subextent has bitvec of 128 blocks

actually, could top out extent size at 1M, use 256 4K blocks per extent

xhd needs to track seghd/subhead info




=head1 FUNCTIONS

=over 4

=item currblock

return the current active block (insert high water mark) for an object

=item firstblock, nextblock

iterate over the set of *used* blocks for an object.  Ignores unused
blocks in last extent

=item countblock

count of all blocks associated with the object.  Includes allocated,
*unused* blocks, plus empty blocks (i.e. blocks with no rows).

=item hasblock 

check if block is associated with an object

=item freetable 

return all of an object's blocks to the freelist

=item flush 

write the contents of block zero to disk.  Need to handle case of
extent lists spread over multiple blocks.


=back


=head2 EXPORT

=head1 TODO

=over 4


=item  need to coalesce adjacent free extents

=item  maintain multiple free lists for performance

=item  better indexing scheme - maybe a btree

=back



=head1 AUTHOR

Jeffrey I. Cohen, jcohen@genezzo.com

=head1 SEE ALSO

perl(1).

Copyright (c) 2006 Jeffrey I Cohen.  All rights reserved.

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
