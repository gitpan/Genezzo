#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/BufCa/RCS/BCFile.pm,v 6.9 2005/02/08 06:34:49 claude Exp claude $
#
# copyright (c) 2003, 2004 Jeffrey I Cohen, all rights reserved, worldwide
#
#
use strict;
use warnings;

package Genezzo::BufCa::BCFile;

use IO::File;
use IO::Handle;
use Genezzo::BufCa::BufCa;
use Genezzo::Util;
use Carp;
use warnings::register;

our @ISA = qw(Genezzo::BufCa::BufCa) ;

# non-exported package globals go here

# initialize package globals, first exported ones
#my $Var1   = '';
#my %Hashit = ();

# then the others (which are still accessible as $Some::Module::stuff)
#$stuff  = '';
#@more   = ();

# all file-scoped lexicals must be created before
# the functions below that use them.

# file-private lexicals go here
#my $priv_var    = '';
#my %secret_hash = ();
# here's a file-private function as a closure,
# callable as &$priv_func;  it cannot be prototyped.
#my $priv_func = sub {
    # stuff goes here.
#};

# make all your functions, whether exported or not;
# remember to put something interesting in the {} stubs
#sub func1      {print "hi";}    # no prototype
#sub func2()    {}    # proto'd void
#sub func3($$)  {}    # proto'd to 2 scalars
#sub func5      {print "ho";}    # no prototype

sub _init
{
    #whoami;
    #greet @_;
    my $self = shift;

    $self->{ __PACKAGE__ . ":FN_ARRAY" } = [];    
    $self->{ __PACKAGE__ . ":FN_HASH"  } = {};    
    $self->{ __PACKAGE__ . ":HITLIST"  } = {};    
    $self->{bc} = Genezzo::BufCa::BufCa->new(@_);
    $self->{cache_hits}   =  0;
    $self->{cache_misses} =  0;
    $self->{read_only}    =  0; # TODO: set for read-only database support

    return 1;
}

sub new 
{
    my $invocant = shift;
    my $class = ref($invocant) || $invocant ; 
    my $self = {};

#    whoami;

    my %args = (@_);

    return undef
        unless (_init($self,%args));

    return bless $self, $class;

} # end new

sub Dump
{
    whoami;
    my $self = shift;
    my $hitlist = $self->{ __PACKAGE__ . ":HITLIST"  };    

    my %hashi = (bc => $self->{bc}->Dump(),
                 cache_hits   => $self->{cache_hits},
                 cache_misses => $self->{cache_misses},
                 hitlist      => scalar keys %{$hitlist}
                 );

    return \%hashi;
}


sub Resize
{
#    whoami;
    my $self = shift;
    return 0
        unless ($self->Flush());
    my $stat = $self->{bc}->Resize(@_);
#    greet $stat;
    return $stat;
}

sub FileReg
{
    my $self = shift;

    local $Genezzo::Util::QUIETWHISPER = 1; # XXX: quiet the whispering

    whoami @_;

    my %required = (
                    FileName => "no FileName !"
                    );
    
    my %args = (
                @_);

    return undef
        unless (Validate(\%args, \%required));

    my $fn_arr = $self->{ __PACKAGE__ . ":FN_ARRAY" };
    my $fn_hsh = $self->{ __PACKAGE__ . ":FN_HASH" };

    # XXX: need a lock here for multithread
    unless (exists($fn_hsh->{$args{FileName}}))
    {
        # array of hashes of file info
        my %th;
        my @headerinfo;
        $th{name} = $args{FileName};

        # XXX: open all handles for now
        $th{fh} = new IO::File "+<$th{name}"
            or die "Could not open $th{name} for writing : $! \n";

        @headerinfo = Genezzo::Util::FileGetHeaderInfo($th{fh}, $th{name});

#        greet @headerinfo;
        return undef
            unless (scalar(@headerinfo));
        $th{hdrsize} = $headerinfo[0];

        push (@{$fn_arr}, \%th);

        # XXX: NOTE: treat filename array as 1 based, vs 0 based 
        # -- use fn_arr[n-1]->name to get filename.
        $fn_hsh->{$args{FileName}} = scalar(@{$fn_arr});
    }   

    return ($fn_hsh->{$args{FileName}})
}


sub _filereadblock
{
#    whoami;
    my ($self, $fname, $fnum, $fh, $bnum, $refbuf, $hdrsize) = @_;

#    greet $fname, $fnum, $fh, $bnum,  $hdrsize; 
    
    my $blocksize = $self->{bc}->{blocksize};

    $fh->sysseek (($hdrsize+($bnum * $blocksize)), 0 )
        or die "bad seek - file $fname : $fnum, block $bnum : $! \n";

    # HOOK: PRE SYSREAD BLOCK

    $fh->sysread ($$refbuf, $blocksize)
        == $blocksize
            or die "bad read - file $fname : $fnum, block $bnum : $! \n";

    # HOOK: POST SYSREAD BLOCK

    if (1)
    {
        # XXX XXX: compute a basic 32 bit checksum
#        my $basicftr = pack($Genezzo::Block::Std::FtrTemplate, 0, 0, 0);
        my $packlen  = $Genezzo::Block::Std::LenFtrTemplate;

        my $skippy = $blocksize-$packlen; # skip to end of buffer
        # get the checksum
        my @outarr = unpack("x$skippy $Genezzo::Block::Std::FtrTemplate", 
                            $$refbuf);

        # zero out the checksum because it wasn't part of the original
        # calculation
#        substr($$refbuf, $blocksize-$packlen, $packlen) = $basicftr;

        # calculate checksum and test if matches stored value
        my $ckTempl  = '%32C' . ($blocksize - $packlen); # skip the footer
        my $cksum = unpack($ckTempl, $$refbuf) % 65535;
        my $ck1 = pop @outarr;
        unless ($cksum == $ck1)
        {
            # XXX XXX: need failure or repair procedure - warn about
            # problem but ignore for now
            my $w1 = "bad read - invalid checksum for file $fname : "
                     . "$fnum, block $bnum : $! \n";
            warn $w1;
        }

    }

    # HOOK: post filereadblock
    
    return (1);
             
}

sub _filewriteblock
{
    my ($self, $fname, $fnum, $fh, $bnum, $refbuf, $hdrsize) = @_;

    return 0
        if ($self->{read_only});

    my $blocksize = $self->{bc}->{blocksize};

    $fh->sysseek (($hdrsize+($bnum * $blocksize)), 0 )
        or die "bad seek - file $fname : $fnum, block $bnum : $! \n";

    # HOOK: init filewriteblock

    # XXX: build a basic header with the file number, block number,
    # etc 
    # XXX XXX fileblockTmpl
    my $basichdr = pack($Genezzo::Block::Std::fileblockTmpl, $fnum, $bnum); 
    my $packlen  = $Genezzo::Block::Std::fbtLen;

    substr($$refbuf, 0, $packlen) = $basichdr;

    if (1)
    {
        # XXX XXX: compute a basic 32 bit checksum 
        # -- see perldoc unpack
#        my $basicftr = pack($Genezzo::Block::Std::FtrTemplate, 0, 0, 0);
        $packlen     = $Genezzo::Block::Std::LenFtrTemplate;

        # zero out the checksum because the old checksum isn't part of
        # the new checksum
#        substr($$refbuf, $blocksize-$packlen, $packlen) = $basicftr;

        my $ckTempl  = '%32C' . ($blocksize - $packlen); # skip the footer
        my $cksum    = unpack($ckTempl, $$refbuf) % 65535;
        my $basicftr = pack($Genezzo::Block::Std::FtrTemplate, 0, 0, $cksum);
        # add the checksum to the end of the block
        substr($$refbuf, $blocksize-$packlen, $packlen) = $basicftr;
    }

    # HOOK: PRE SYSWRITE BLOCK

    $fh->syswrite ($$refbuf,  $blocksize)
        == $blocksize
    or die "bad write - file $fname : $fnum, block $bnum : $! \n";

    # HOOK: POST SYSWRITE BLOCK

    return (1);
}

sub ReadBlock 
{
    my $self   = shift;

#    whoami @_;

    my %required = (
                    filenum  => "no filenum !",
                    blocknum => "no blocknum !"
                    );
                    
#    my %optional ;# XXX XXX XXX: dbh_ctx

    my %args = (
                @_);

    return undef
        unless (Validate(\%args, \%required));

    my $fn_arr = $self->{ __PACKAGE__ . ":FN_ARRAY" };
    my $fnum   =  $args{filenum};

    return undef
        unless (NumVal(
                       verbose => warnings::enabled(),
                       name => "filenum",
                       val => $fnum,
                       MIN => 0,
                       MAX => (scalar(@{$fn_arr}) + 1))) ;

    my $hitlist = $self->{ __PACKAGE__ . ":HITLIST"  };    
    my $bnum  = $args{blocknum};

    # cache hit
    if (exists($hitlist->{"FILE:" . "$fnum" . ":". "$bnum"}))
    {
#        whisper "hit!";
        $self->{cache_hits} +=  1;

        my $bcblocknum = $hitlist->{"FILE:" . "$fnum" . ":". "$bnum"};
        return $self->{bc}->ReadBlock(blocknum => $bcblocknum);
    }

    # miss
#    whisper "miss!";
    $self->{cache_misses} +=  1;

    my $fname  = $fn_arr->[$fnum-1]->{name};
    my $fh     = $fn_arr->[$fnum-1]->{fh};
    my $fhdrsz = $fn_arr->[$fnum-1]->{hdrsize};

    my $thing = $self->{bc}->GetFree();

    unless (2 == scalar(@{$thing}))
    {
        whisper "no free blocks!";

        greet $hitlist;
        return undef;
    }
    
    my $bceref     = pop (@{$thing});
    my $bcblocknum = pop (@{$thing});

    my $bce = $$bceref;

    if (1) # need to clean the hitlist even if not dirty
    {
#        greet $hitlist;

        if (exists($hitlist->{"BC:" . "$bcblocknum"}))
        {
            my $fileinfo = $hitlist->{"BC:" . "$bcblocknum"};

            my ($ofnum, $obnum) = ($fileinfo =~ m/FILE:(\d.*):(\d.*)/);
#            greet $fileinfo, $ofnum, $obnum;
            delete $hitlist->{$fileinfo};
#            greet $hitlist;
            if ($bce->_dirty())
            {
                my $ofname  = $fn_arr->[$ofnum-1]->{name};
                my $ofh     = $fn_arr->[$ofnum-1]->{fh};
                my $ofhdrsz = $fn_arr->[$ofnum-1]->{hdrsize};

                return (undef)
                    unless (
                            $self->_filewriteblock(
                                                   $ofname, 
                                                   $ofnum, 
                                                   $ofh, 
                                                   $obnum, 
                                                   $bce->{bigbuf},
                                                   $ofhdrsz
                                                   )
                            );
            }
        }
    }

    my $fileinfo = "FILE:" . "$fnum" . ":". "$bnum";
    $hitlist->{$fileinfo}             = $bcblocknum;
    $hitlist->{"BC:" . "$bcblocknum"} = $fileinfo;

    # get the hash of bce information and update with filenum, blocknum
    my $infoh = $bce->GetInfo();

    # update the GetInfo *before* the fileread so locking code has some
    # place to look up the information
    $infoh->{filenum}  = $fnum;
    $infoh->{blocknum} = $bnum;

    $bce->_fileread(1);
    my $readstat =  $self->_filereadblock($fname, $fnum, $fh, $bnum, 
                                          $bce->{bigbuf}, $fhdrsz);
    $bce->_fileread(0);
    # new block is not dirty
    $bce->_dirty(0);

    # XXX XXX XXX: error -- need to clean the hitlist!!
    return (undef)
        unless ($readstat);

#    greet $hitlist;
    return $self->{bc}->ReadBlock(blocknum => $bcblocknum);
} # end ReadBlock


sub WriteBlock 
{
    my $self   = shift;

#    whoami @_;

    my %required = (
                    filenum  => "no filenum !",
                    blocknum => "no blocknum !"
                    );

#    my %optional ;# XXX XXX XXX: dbh_ctx

    my %args = (
                @_);

    return undef
        unless (Validate(\%args, \%required));

    my $fn_arr = $self->{ __PACKAGE__ . ":FN_ARRAY" };
    my $fnum   =  $args{filenum};

    return undef
        unless (NumVal(
                       verbose => warnings::enabled(),
                       name => "filenum",
                       val => $fnum,
                       MIN => 0,
                       MAX => (scalar(@{$fn_arr}) + 1))) ;

    my $hitlist = $self->{ __PACKAGE__ . ":HITLIST"  };    
    my $bnum  = $args{blocknum};

    return 1
        unless (exists($hitlist->{"FILE:" . "$fnum" . ":". "$bnum"}));
    # cache hit

    my $bcblocknum = $hitlist->{"FILE:" . "$fnum" . ":". "$bnum"};
    my $bceref =  $self->{bc}->ReadBlock(blocknum => $bcblocknum);
    my $bce = $$bceref;

    if ($bce->_dirty())
    {
        my $fname  = $fn_arr->[$fnum-1]->{name};
        my $fh     = $fn_arr->[$fnum-1]->{fh};
        my $fhdrsz = $fn_arr->[$fnum-1]->{hdrsize};

        return (0)
            unless (
                    $self->_filewriteblock($fname, $fnum, $fh, $bnum, 
                                           $bce->{bigbuf}, $fhdrsz)
                    );
    }
    $bce->_dirty(0);

    return 1;

} # end WriteBlock

sub Flush 
{
    my $self   = shift;

    whoami;

    my $hitlist = $self->{ __PACKAGE__ . ":HITLIST"  };    
    my $fn_arr  = $self->{ __PACKAGE__ . ":FN_ARRAY" };

    my %sync_list;

    # HOOK: PRE FLUSH BCFILE

    while (my ($kk, $vv) = each (%{$hitlist}))
    {
        next if ($kk !~ /^FILE/);

        my ($fnum, $bnum) = ($kk =~ m/FILE:(\d.*):(\d.*)/);

        my $bceref =  $self->{bc}->ReadBlock(blocknum => $vv);
        my $bce = $$bceref;

        if ($bce->_dirty())
        {
            my $fname  = $fn_arr->[$fnum-1]->{name};
            my $fh     = $fn_arr->[$fnum-1]->{fh};
            my $fhdrsz = $fn_arr->[$fnum-1]->{hdrsize};

            $sync_list{$fnum} = 1;

            whisper "write dirty block : $fname - $fnum : $bnum";

            return (0)
                unless (
                        $self->_filewriteblock($fname, $fnum, $fh, $bnum, 
                                               $bce->{bigbuf}, $fhdrsz)
                        );
        }
        $bce->_dirty(0);
    }

    for my $fnum (keys (%sync_list))
    {
        # sync the file handles - normally, can bcfile can buffer
        # writes, but in this case we want to assure they get written
        # before commit
        #
        # Note: sync is an IO::Handle method inherited by IO::File
        my $fname  = $fn_arr->[$fnum-1]->{name};
        my $fh     = $fn_arr->[$fnum-1]->{fh};

        whisper "failed to sync $fname"
            unless ($fh->sync); # should be "0 but true"
    }

    # HOOK: POST FLUSH BCFILE

    return 1;
#    greet $hitlist;
    
} # end flush

sub Rollback 
{
    my $self   = shift;

    whoami;

    my $hitlist = $self->{ __PACKAGE__ . ":HITLIST"  };    
    my $fn_arr  = $self->{ __PACKAGE__ . ":FN_ARRAY" };

    # HOOK: PRE ROLLBACK BCFILE

    while (my ($kk, $vv) = each (%{$hitlist}))
    {
        next if ($kk !~ /^FILE/);

        my ($fnum, $bnum) = ($kk =~ m/FILE:(\d.*):(\d.*)/);

        my $bceref =  $self->{bc}->ReadBlock(blocknum => $vv);
        my $bce = $$bceref;

        if ($bce->_dirty())
        {
            my $fname  = $fn_arr->[$fnum-1]->{name};
            my $fh     = $fn_arr->[$fnum-1]->{fh};
            my $fhdrsz = $fn_arr->[$fnum-1]->{hdrsize};

            whisper "replace dirty block : $fname - $fnum : $bnum";

            $bce->_dirty(0);

            return (0)
                unless (
                        $self->_filereadblock($fname, $fnum, $fh, $bnum, 
                                              $bce->{bigbuf}, $fhdrsz)
                        );
        }
    }

    # HOOK: POST ROLLBACK BCFILE

    return 1;
#    greet $hitlist;
    
}

sub BCGrowFile
{
    whoami;
    my ($self, $filenumber, $startblock, $numblocks) = @_;

    my $fnum = $filenumber;
    my $fn_arr  = $self->{ __PACKAGE__ . ":FN_ARRAY" };
    my $blocksize = $self->{bc}->{blocksize};
    my $fname  = $fn_arr->[$fnum-1]->{name};
    my $fh     = $fn_arr->[$fnum-1]->{fh};
    my $fhdrsz = $fn_arr->[$fnum-1]->{hdrsize};
 
    my $packstr  = "\0" x $blocksize ; # fill with nulls

    my @outi;

    push @outi, $startblock;

    for my $ii (0..($numblocks - 1))
    {
        my $bnum = $startblock + $ii;
#        greet "new block $bnum";
        return @outi
            unless (
                    $self->_filewriteblock($fname, $fnum, $fh, $bnum, 
                                           \$packstr, $fhdrsz)
                    );
        $outi[1] = $ii + 1; # number of blocks added
    }
    return @outi; # starting block number, number of new blocks
}

sub DESTROY
{
    my $self   = shift;
#    whoami;

    if (exists($self->{bc}))
    {
        $self->{bc} = ();
    }

}

END { }       # module clean-up code here (global destructor)

## YOUR CODE GOES HERE

1;  # don't forget to return a true value from the file

=head1 NAME

 Genezzo::BufCa::BCFile.pm - A simple in-memory buffer cache for 
 multiple files for a single process, without locking.    

=head1 SYNOPSIS

 use Genezzo::BufCa::BCFile;
 
 # get a buffer cache
 my $bc = Genezzo::BufCa::BCFile->new(blocksize => 10, numblocks => 5);

 # register a file
 my $fileno = Genezzo::BufCa::BCFile->FileReg(FileName => 'file.dat');

 # get back some block 
 $bceref = $bc->ReadBlock(filenum  => $fileno,
                          blocknum => $blocknum);
 $bce = $$bceref;

=head1 DESCRIPTION

 The file buffer cache is a simple module designed to form the
 basis of a more complicated multi-process buffer cache
 with locking.  The buffer cache contains a number of Buffer Cache
 Elements (BCEs), a special wrapper class for simple byte buffers
 (blocks).  See L<Genezzo::BufCa::BufCa>.

 Note that this module does not perform space management or allocation 
 within the files -- it only reads and writes the blocks.  The caller
 is responsible for managing the contents of the file.
 
=head1 FUNCTIONS
  
=over 4

=item new

 Takes arguments blocksize (required, in bytes), numblocks (10 by
 default).  Returns a new buffer cache of the specified number of
 blocks of size blocksize.

=item FileReg

 Register a file with the cache -- returns a file number.  Reregistering
 a file should return the same number.

=item ReadBlock  

 Takes argument blocknum, which must be a valid block number, and
 the argument filenum, which must be a valid file number.  If the
 block is in memory it returns the bceref.  If the block is not in
 the cache it fetches it from disk into an unused block.  If the 
 unused block is dirty, then ReadBlock writes it out first.  
 Fails if all blocks are in use.

=item WriteBlock 

 Write a block to disk.  Not really necessary -- ReadBlock will
 flush some dirty blocks to disk automatically, and Flush
 will write all dirty blocks to disk.  

=item Flush

 Write all dirty blocks to disk.

=item Rollback

 Discard all dirty blocks and replace with blocks from disk..

=back

=head2 EXPORT

 None by default.

=head1 LIMITATIONS

Currently requires 2 blocks per open file.

=head1 TODO

=over 4

=item  note that _fileread could just be part of GetInfo

=item  need to move TSExtendFile functionality here if want to overload
       syswrite with encryption

=item  read_only database support

=item  buffer cache block zero should contain description of buffer cache 
       layout

=item  need a way to free blocks associated with a file that is not
       currently in use

=back


=head1 AUTHOR

 Jeffrey I. Cohen, jcohen@genezzo.com

=head1 SEE ALSO

perl(1).

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
