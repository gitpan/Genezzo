# Copyright (c) 2003, 2004 Jeffrey I Cohen.  All rights reserved.
#
# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl test.pl'

######################### We start with some black magic to print on failure.

# Change 1..1 below to 1..last_test_to_print .
# (It may become useful if the test is moved to ./t subdirectory.)

BEGIN { $| = 1; print "1..30\n"; }
END {print "not ok 1\n" unless $loaded;}
use Genezzo::GenDBI;
$loaded = 1;
print "ok 1\n";

######################### End of black magic.

# Insert your test code below (better if it prints "ok 13"
# (correspondingly "not ok 13") depending on the success of chunk 13
# of the test code):
use strict;
use warnings;
use File::Path;
use File::Spec;

my $TEST_COUNT;

$TEST_COUNT = 2;

my $dbinit   = 1;
my $gnz_home = File::Spec->catdir("t", "gnz_home");
rmtree($gnz_home, 1, 1);
#mkpath($gnz_home, 1, 0755);


{
    my $fb = Genezzo::GenDBI->new(exe => $0, 
                             gnz_home => $gnz_home, 
                             dbinit => $dbinit);

    unless (defined($fb))
    {
        not_ok ("could not create database");
        exit 1;
    }
    ok();
    $dbinit = 0;

}

{
    use Genezzo::Util;

    my $fb = Genezzo::GenDBI->new(exe => $0, 
                             gnz_home => $gnz_home, 
                             dbinit => $dbinit);

    unless (defined($fb))
    {
        not_ok ("could not find database");
        exit 1;
    }
    ok();
    $dbinit = 0;

}

{
    use Genezzo::Util;

    my $dbh = Genezzo::GenDBI->connect($gnz_home, "NOUSER", "NOPASSWORD");

    unless (defined($dbh))
    {
        not_ok ("could not find database");
        exit 1;
    }
    ok();

    if ($dbh->do("startup"))
    {       
        ok();
    }
    else
    {
        not_ok ("could not startup");
    }

    if ($dbh->do("create table havok (hid n, modname c, owner c, creationdate c)"))
    {       
        ok();
    }
    else
    {
        not_ok ("could not create table havok");
    }
    if ($dbh->do("create table UserExtend (xid n, xtype c, xname c, args c, owner c, creationdate c)"))
    {       
        ok();
    }
    else
    {
        not_ok ("could not create table UserExtend");
    }

    my $now = now();

    if ($dbh->do(
        "insert into havok values (1, Genezzo::Havok::UserExtend, SYSTEM, $now)"))
    {       
        ok();
    }
    else
    {
        not_ok ("could not insert into havok");
    }

    my $soundx = '"qw(soundex)"'; # quoting quotes

    if ($dbh->do(
        "insert into UserExtend values (1, require, Text::Soundex, $soundx, SYSTEM, $now)"))
    {       
        ok();
    }
    else
    {
        not_ok ("could not insert into UserExtend");
    }

    if ($dbh->do("commit"))
    {       
        ok();
    }
    else
    {
        not_ok ("could not commit");
    }
    if ($dbh->do("create table sonictest (sname c)"))
    {       
        ok();
    }
    else
    {
        not_ok ("could not create table sonictest");
    }
    if ($dbh->do(
        "insert into sonictest values (Euler, Ellery, Gauss, Ghosh)"))
    {       
        ok();
    }
    else
    {
        not_ok ("could not insert into sonictest");
    }
    if ($dbh->do(
        "insert into sonictest values (Hilbert, Heilbronn, Knuth, Kant)"))
    {       
        ok();
    }
    else
    {
        not_ok ("could not insert into sonictest");
    }
    if ($dbh->do(
        "insert into sonictest values (Lloyd, Ladd, Lukasiewicz, Lissajous)"))
    {       
        ok();
    }
    else
    {
        not_ok ("could not insert into sonictest");
    }
    if ($dbh->do("commit"))
    {       
        ok();
    }
    else
    {
        not_ok ("could not commit");
    }
    if ($dbh->do("shutdown"))
    {
        ok();
    }
    else
    {
        not_ok ("could not shutdown");
    }
    if ($dbh->do("startup"))
    {       
        ok();
    }
    else
    {
        not_ok ("could not startup");
    }


    # Knuth's test data for soundex

    my @ary = qw(
Euler
Ellery
Gauss
Ghosh
Hilbert
Heilbronn
Knuth
Kant
Lloyd
Ladd
Lukasiewicz
Lissajous
                 );


    while (scalar(@ary) > 1)
    {
        my $a1 = shift @ary;
        my $a2 = shift @ary;

        my $s1 = 
               "select sname from sonictest where " .
               ' Text::Soundex::soundex(sname) = ' .
               ' Text::Soundex::soundex("'. $a2 . '") ' ;

        greet $s1;

        my $sth = $dbh->prepare($s1);

        print $sth->execute(), " rows \n";

        my @f1 = $sth->fetchrow_array();
        unless (scalar(@f1))
        {
            not_ok ("no match for first fetch $a1, $a2");
        }

        while (scalar(@f1))
        {
            if ($f1[0] =~ m/$a1|$a2/)
            {
                ok();
            }
            else
            {
                not_ok ("no match for fetch $a1, $a2");
            }
            @f1 = $sth->fetchrow_array();
        }

    }


    if ($dbh->do("shutdown"))
    {
        ok();
    }
    else
    {
        not_ok ("could not shutdown");
    }


}


sub ok
{
    print "ok $TEST_COUNT\n";
    
    $TEST_COUNT++;
}


sub not_ok
{
    my ( $message ) = @_;
    
    print "not ok $TEST_COUNT #  $message\n";
        
        $TEST_COUNT++;
}


sub skip
{
    my ( $message ) = @_;
    
    print "ok $TEST_COUNT # skipped: $message\n";
        
        $TEST_COUNT++;
}

sub now # from time_iso8601
{
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
        localtime(time);
    
    # example: 2002-12-19T14:02:57
    
    # year is YYYY-1900, mon in (0..11)

    my $tstr = sprintf ("%04d-%02d-%02dT%02d:%02d:%02d", 
                        ($year + 1900) , $mon + 1, $mday, $hour, $min, $sec);
    return $tstr;
}
