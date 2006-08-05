#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/Havok/RCS/DebugUtils.pm,v 1.1 2006/08/02 05:46:23 claude Exp claude $
#
# copyright (c) 2006 Jeffrey I Cohen, all rights reserved, worldwide
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
    $VERSION = do { my @r = (q$Revision: 1.1 $ =~ /\d+/g); sprintf "%d."."%02d" x $#r, @r }; # must be all one line, for MakeMaker

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
    };

    # DML is an array, not a hash

    my $now = 
    do { my @r = (q$Date: 2006/08/02 05:46:23 $ =~ m|Date:(\s+)(\d+)/(\d+)/(\d+)(\s+)(\d+):(\d+):(\d+)|); sprintf ("%04d-%02d-%02dT%02d:%02d:%02d", $r[1],$r[2],$r[3],$r[5],$r[6],$r[7]); };


    my %tabdefs = ();
    $MAKEDEPS->{'TABLEDEFS'} = \%tabdefs;

    my @perl_funcs = qw(
                        bcfiledump
                        metadump
                        );


    my @ins1;
    # XXX XXX XXX XXX: need "select COUNT(*) from user_functions"
    my $ccnt = 49;
    for my $pfunc (@perl_funcs)
    {
        my $bigstr = "i user_functions $ccnt require $pak1 " 
            . "sql_func_" . $pfunc . " SYSTEM $now 0 HASH $pfunc";
        push @ins1, $bigstr;
        $ccnt++;
    }

    # if check returns 0 rows then proceed with install
    $MAKEDEPS->{'DML'} = [
                          { check => [
                                      "select * from user_functions where xname = \'$pak1\'"
                                      ],
                            install => \@ins1
                            }
                          ];

#    print Data::Dumper->Dump([$MAKEDEPS]);
}

sub MakeYML
{
    use Genezzo::Havok;

    my $makedp = $MAKEDEPS;

    return Genezzo::Havok::MakeYML($makedp);
}

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
sub sql_func_metadump
{
    my %args= @_;

    my $dict = $args{dict};
    my $dbh  = $args{dbh};
    my $fn_args = $args{function_args};

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
        my $fileno = $file_info->[1];
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

                                print "$id: ",
                                Data::Dumper->Dump($val), "\n";
                            }

                        }

                    }
                }

            }
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

Genezzo::Havok::DebugUtils - debug functions

=head1 SYNOPSIS

select HavokUse('Genezzo::Havok::DebugUtils') from dual;

=head1 DESCRIPTION

=head1 ARGUMENTS

=head1 FUNCTIONS

=over 4

=item  bcfiledump


=item  metadump


=back

=head2 EXPORT

=over 4


=back


=head1 LIMITATIONS

=head1 AUTHOR

Jeffrey I. Cohen, jcohen@genezzo.com

=head1 SEE ALSO

L<perl(1)>.

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
