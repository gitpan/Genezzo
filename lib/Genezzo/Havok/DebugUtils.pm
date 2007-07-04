#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/Havok/RCS/DebugUtils.pm,v 1.6 2007/06/26 08:25:52 claude Exp claude $
#
# copyright (c) 2006, 2007 Jeffrey I Cohen, all rights reserved, worldwide
#
#
package Genezzo::Havok::DebugUtils;
require Exporter;

@ISA = qw(Exporter);
@EXPORT = qw(&sql_func_metadump
             );

use Genezzo::Util;

use strict;
use warnings;

use Carp;

our $VERSION;
our $MAKEDEPS;

BEGIN {
    $VERSION = do { my @r = (q$Revision: 1.6 $ =~ /\d+/g); sprintf "%d."."%02d" x $#r, @r }; # must be all one line, for MakeMaker

    my $pak1  = __PACKAGE__;
    $MAKEDEPS = {
        'NAME'     => $pak1,
        'ABSTRACT' => ' ',
        'AUTHOR'   => 'Jeffrey I Cohen (jcohen@cpan.org)',
        'LICENSE'  => 'gpl',
        'VERSION'  =>  $VERSION,
        }; # end makedeps

    $MAKEDEPS->{'PREREQ_HAVOK'} = {
        'Genezzo::Havok::UserFunctions' => '0.0',
        'Genezzo::Havok::Utils' => '0.0',
        'Genezzo::Havok::SysHelp' => '0.0',
    };

    # DML is an array, not a hash

    my $now = 
    do { my @r = (q$Date: 2007/06/26 08:25:52 $ =~ m|Date:(\s+)(\d+)/(\d+)/(\d+)(\s+)(\d+):(\d+):(\d+)|); sprintf ("%04d-%02d-%02dT%02d:%02d:%02d", $r[1],$r[2],$r[3],$r[5],$r[6],$r[7]); };


    my %tabdefs = ();
    $MAKEDEPS->{'TABLEDEFS'} = \%tabdefs;

    my @perl_funcs = qw(
                        bcfiledump
                        metadump
                        blockdump
                        gnz_history
                        );


    my @ins1;
    my $ccnt = 1;
    for my $pfunc (@perl_funcs)
    {
        my %attr = (module => $pak1, 
                    function => "sql_func_" . $pfunc,
                    creationdate => $now,
                    argstyle => 'HASH',
                    sqlname => $pfunc);

        my @attr_list;
        while ( my ($kk, $vv) = each (%attr))
        {
            push @attr_list, '\'' . $kk . '=' . $vv . '\'';
        }

        my $bigstr = "select add_user_function(" . join(", ", @attr_list) .
            ") from dual";
        push @ins1, $bigstr;
        $ccnt++;
    }

    # add help for DebugUtils
    push @ins1, "select add_help(\'Genezzo::Havok::DebugUtils\') from dual";


    # if check returns 0 rows then proceed with install
    $MAKEDEPS->{'DML'} = [
                          { check => [
                                      "select * from user_functions where xname = \'$pak1\'"
                                      ],
                            install => \@ins1
                            }
                          ];

#    print Data::Dumper->Dump([$MAKEDEPS]);
} # end BEGIN

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

sub MakeYML
{
    use Genezzo::Havok;

    my $makedp = $MAKEDEPS;

    return Genezzo::Havok::MakeYML($makedp);
}

sub getpod
{
    my $bigHelp;
    ($bigHelp = <<'EOF_HELP') =~ s/^\#//gm;
#=head1 Debug_Utility_Functions
#
#=head2  bcfiledump : bcfiledump()
#
#Dump state for all active buffer caches and their associated tablespace
#files
#
#=head2  metadump : metadump(filenum, blocknum)
#
#Dump the metadata rows for the specified block.
#
#=head2 blockdump : blockdump(filenum, blocknum)
#
#Dump the block header and row state.  Each row may several status flags
#which are:
#
# X DELETED (vs not)
# M Metadata (vs data)
# L Locked (vs not) (currently unused).
# H Head  
# T Tail 
# / middle row piece (neither head nor tail)
# ISNULL (vs not null)
#
#Deleted rows still take up space in the block, but they can be "compacted" 
#to a minimal length.
#
#Metadata rows are used for special configuration information, not
#regular user data.
#
#A row can be split across multiple blocks.  The first part of the row
#is the head piece, and the last part is the tail piece.  
#
#The block layer uses a special flag to track completely null entries.
#However, most Genezzo blocks are organized as "packed" rows of
#multiple values, which use a separate mechanism to track individual
#null columns.
#
#=head2  gnz_history : gnz_history()
#
#Save the interactive command history to ~/.gnz_history.  The history is 
#automatically reloaded for each session.  If gnz_history('autosave') is 
#specified, the history is saved when you quit the interactive session.
#
#
EOF_HELP

    my $msg = $bigHelp;

    return $msg;

} # end getpod



sub sql_func_bcfiledump
{
    my %args= @_;

    my $dict = $args{dict};
    my $dbh  = $args{dbh};
    my $fn_args = $args{function_args};

    while (my ($kk, $vv) = each (%{$dict->{tablespaces}}))
    {
        print "tablespace: $kk\n";
        my $bc1;
        if (exists($vv->{tsref})
            && exists($vv->{tsref}->{the_ts})
            && exists($vv->{tsref}->{the_ts}->{bc}))
        {
            $bc1 = $vv->{tsref}->{the_ts}->{bc};

            print Data::Dumper->Dump($bc1->_get_fn_array()), "\n";
            print Data::Dumper->Dump([$bc1->_get_fn_hash()]), "\n";
        }
    }

    return 1;
}

sub _meta_row_dump
{
    my ($id, $val) = @_;

    print "$id: ",
    Data::Dumper->Dump($val), "\n";

    if ($id =~ m/^X1A$/)
    {
        print Genezzo::SpaceMan::SMExtent->_meta_row_dump_X1A($val);
    }
    elsif ($id =~ m/^X1B$/)
    {
        print Genezzo::SpaceMan::SMExtent->_meta_row_dump_X1B($val);        
    }
    elsif ($id =~ m/^XHA$/)
    {
        print Genezzo::SpaceMan::SMExtent->_meta_row_dump_XHA($val);        
    }
    elsif ($id =~ m/^XHP$/)
    {
        print Genezzo::SpaceMan::SMExtent->_meta_row_dump_XHP($val);        
    }
    

}

sub _block_func
{
    my %args= @_;

    my $dict = $args{dict};
    my $dbh  = $args{dbh};
    my $fn_args = $args{function_args};

    my $block_func = $args{block_func};

    my @blocklist;
    
    if (scalar(@{$fn_args}) > 1)
    {
        my $fidx = $fn_args->[0];
        my $sth =
            $dbh->prepare("select tsp.tsname, tsp.tsid, tfil.filename from _tsfiles tfil, _tspace tsp where tfil.fileidx = $fidx and tsp.tsid = tfil.tsid");

        if ($sth) 
        {
            $sth->execute();

            while (1) 
            {
                my @lastfetch = $sth->fetchrow_array();

                last
                    unless (scalar(@lastfetch));

                my $ggg = [];
                push @{$ggg}, @lastfetch;
                push @blocklist, $ggg;
            }
        } # end if sth
    }
    print Data::Dumper->Dump(\@blocklist), "\n";

    for my $file_info (@blocklist)
    {
        my $tsname = $file_info->[0];
#        my $fileno = $file_info->[1]; # ? tsid, not fileno...
        my $fileno = $fn_args->[0]; # XXX XXX
        my $fname  = $file_info->[2];

        if (exists($dict->{tablespaces}->{$tsname}))
        {
            my $vv = $dict->{tablespaces}->{$tsname};

            my $bc1;
            if (exists($vv->{tsref})
                && exists($vv->{tsref}->{the_ts})
                && exists($vv->{tsref}->{the_ts}->{bc}))
            {
                $bc1 = $vv->{tsref}->{the_ts}->{bc};

                print Data::Dumper->Dump($bc1->_get_fn_array()), "\n";
                print Data::Dumper->Dump([$bc1->_get_fn_hash()]), "\n";

                my $blockno = $fn_args->[1];
                
                if (defined($blockno))
                {
                    my $bceref = $bc1->ReadBlock(filenum  => $fileno,
                                                 blocknum => $blockno);

                    if ($bceref)
                    {
                        my $bce = ${$bceref};

                        my $ROW_DIR_BLOCK_CLASS  = 'Genezzo::Row::RSBlock';
                        my $RDBlock_Class        = "Genezzo::Block::RDBlock",
                        my %tiebufa;
                        # tie array to buffer
                        my $rowd = 
                            tie %tiebufa, $ROW_DIR_BLOCK_CLASS,
                            (RDBlock_Class => $RDBlock_Class,
                             blocknum  => $blockno,
                             refbufstr => $bce->{bigbuf},
                             # XXX XXX : get blocksize from bce!!
                             blocksize => $bce->{blocksize}
                             );

                        if ($block_func eq 'metadump')
                        {

                            my $metazero = $rowd->_fetchmeta(undef, 0);

                            print Data::Dumper->Dump([$metazero]), "\n";

                            my @row = UnPackRow($metazero, $Genezzo::Util::UNPACK_TEMPL_ARR);
                        
                            print Data::Dumper->Dump(\@row), "\n";

                            for my $col1 (@row)
                            {
                                my @foo = split(':', $col1);

                                if (scalar(@foo) && ($foo[0] ne '#'))
                                {
                                    my $id  = $foo[0];
                                    my $val = $rowd->_get_meta_row($id);
                                
                                    _meta_row_dump($id, $val);

                                }

                            }
                        }
                        else
                        {
                            my $msg = $rowd->BlockInfoString();

                            # XXX XXX:
                            print $msg;
                            

                        }

                    } # end if bceref
                }

            }
        }
        
    }

    return 1;
}


# fileno, blockno
sub sql_func_metadump
{
    my %args= @_;

    $args{block_func} = 'metadump';

    return _block_func(%args);
}

# fileno, blockno
sub sql_func_blockdump
{
    my %args= @_;

    $args{block_func} = 'blockdump';

    return _block_func(%args);
}


# save the interactive history.  use 'autosave' to save on quit.
# TODO: make autosave "sticky" so history is always saved for current
# and all subsequent sessions.  Delete ~/.gnz_history to clear.
# Probably need an option to do this, as well as disable autosave...
sub sql_func_gnz_history
{
    my %args= @_;

    my $dict = $args{dict};
    my $dbh  = $args{dbh};
    my $fn_args = $args{function_args};

    return $dbh->SaveHistory($fn_args);
}


END { }       # module clean-up code here (global destructor)

## YOUR CODE GOES HERE

1;  # don't forget to return a true value from the file

__END__
# Below is stub documentation for your module. You better edit it!

=head1 NAME

Genezzo::Havok::DebugUtils - debug functions

=head1 SYNOPSIS

select HavokUse('Genezzo::Havok::DebugUtils') from dual;

=head1 DESCRIPTION

Special debugging utility functions.

=head1 ARGUMENTS

=head1 FUNCTIONS

=over 4

=item  bcfiledump


=item  metadump


=item  blockdump


=item gnz_history

=back

=head2 EXPORT

=over 4


=back


=head1 LIMITATIONS

=head1 AUTHOR

Jeffrey I. Cohen, jcohen@genezzo.com

=head1 SEE ALSO

L<perl(1)>.

Copyright (c) 2006, 2007 Jeffrey I Cohen.  All rights reserved.

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
