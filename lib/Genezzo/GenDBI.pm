#!/usr/bin/perl
#
# $Header: /Users/claude/fuzz/lib/Genezzo/RCS/GenDBI.pm,v 6.21 2005/01/23 09:59:40 claude Exp claude $
#
# copyright (c) 2003,2004,2005 Jeffrey I Cohen, all rights reserved, worldwide
#
#
package Genezzo::GenDBI;

require 5.005_62;
use strict;
use warnings;

require Exporter;

use Carp;
use Data::Dumper ;
use Genezzo::Feeble;
use Genezzo::Dict;
use Genezzo::Util;
#use SQL::Statement;
use Term::ReadLine;
use Text::ParseWords qw(shellwords quotewords parse_line);
use warnings::register;

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use F2 ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.

BEGIN {
    use Exporter   ();

    our (@ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS);

    @ISA         = qw(Exporter);
    %EXPORT_TAGS = ( 'all' => [ qw(
                                   $VERSION $RELSTATUS $RELDATE errstr
                                   ) ] 
                     );

    @EXPORT_OK = qw( @{ $EXPORT_TAGS{'all'} } );

    @EXPORT = qw( );
	
}

our $VERSION   = '0.33';
our $RELSTATUS = 'Alpha'; # release status
# grab the code check-in date and convert to YYYYMMDD
our $RELDATE   = 
    do { my @r = (q$Date: 2005/01/23 09:59:40 $ =~ m|Date:(\s+)(\d+)/(\d+)/(\d+)|); sprintf ("%04d%02d%02d", $r[1],$r[2],$r[3]); };

our $errstr; # DBI errstr

#
# GZERR: the GeneZzo ERRor message handler
#
# You can define or redefine an error message handler for Genezzo 
# 
# Arguments:
#
# msg (required): an actual message that you can print, carp about, or log.
# severity (optional): mainly to distinguish between informational
#   messages and actual errors.  The current set of severities are INFO,
#   WARN, ERROR, and FATAL, though I'll probably add DEBUG or DBG to replace 
#   the "whisper" messages.  
# self (optional): for object-oriented packages, adding a GZERR attribute 
#   to the $self is a bit cleaner way of propagating a common error routine
#   to subsequent classes in your hierarchy.
#
# Specifications:
# Your error handler should do something when it gets a message.  
# For example, the gendba.pl error handler prints INFO messages like 
# "5 rows selected" and it flags errors with a prefix like WARNING or ERROR.
# If you use the dbi-style connect to obtain a database handle, the default
# handler ignores INFO msgs, but prints all errors and warnings.
#
# gendba.pl supplies its own error handler when it calls GenDBI::new, 
# and GenDBI::connect (the DBI-style interface) has its own error handler,
# which can be overridden in the attribute hash
#
# The default error handler declared here is typically not used.
#
# dbi gzerr doesn't call $self->gzerr to eliminate recursive hell
our $dbi_gzerr = sub {
    my %args = (@_);

    return 
        unless (exists($args{msg}));

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
    # add a newline if necessary
    print "\n" unless $args{msg}=~/\n$/;
#    carp $args{msg}
#      if (warnings::enabled() && $warn);
    
};

our $GZERR = sub {
    my %args = (@_);

    # use the error routine supplied to GenDBI class if it exists,
    # else use package error handler (dbi_gzerr)
    if (exists($args{self}))
    {
        my $self = $args{self};
        if (defined($self) && exists($self->{GZERR}))
        {
            my $err_cb = $self->{GZERR};
            return &$err_cb(%args);
        }
    }
    return &$dbi_gzerr(%args);
};

# NOTE: turn off "whisper" debug information.  
# Use "def _QUIETWHISPER=0" to re-enable if necessary.
$Genezzo::Util::QUIETWHISPER  = 1; # XXX XXX XXX XXX
$Genezzo::Util::USECARP       = 0;
#$Genezzo::Util::WHISPERPREFIX = "baz: ";
#$Genezzo::Util::WHISPERPREFIX = undef;
#$Genezzo::Util::WHISPER_PRINT = sub { print "baz2: ", @_ ; };

# Preloaded methods go here.

sub _init
{
    my $self = shift;
    my %args = (@_);

    $self->{caller} = $args{exe}
       if (exists($args{exe}));

# the data dictionary
    $self->{dictobj} = ();
    
    $self->{bigstatement} = ();
    $self->{endwait} = 0;
    
    my @histlist = ();
    
    $self->{histlist} = \@histlist;
    $self->{maxhist}  = 100;
    $self->{histcounter} = 1;

    if ((exists($args{gnz_home}))
        && (defined($args{gnz_home}))
        && (length($args{gnz_home})))
    {
        $self->{gnz_home} = $args{gnz_home};
    }
    else
    {
        $self->{gnz_home} = $ENV{GNZ_HOME} || 
            File::Spec->catdir($ENV{HOME} , 'gnz_home');
    }
#    print "$self->{gnz_home}\n";

    my %nargs;
    if (exists($args{GZERR})) # pass the error reporting routine
    {
        $nargs{GZERR} = $args{GZERR};
    }
    $self->{feeble}  = Genezzo::Feeble->new(%nargs); # build a feeble parser
    return 0
        unless (defined($self->{feeble}));

    my $init_db = 0;

    if ((exists($args{dbinit}))
        && (defined($args{dbinit}))
        && (length($args{dbinit})))
    {
        $init_db = $args{dbinit};
    }

    my %dictargs;

    if ((exists($args{defs}))
        && (defined($args{defs}))
        )
    {
        my %legitdefs = 
            (
             blocksize => 
             "size of a database block in bytes, e.g. blocksize=4k",
             force_init_db =>
             "set =1 to overwrite (and destroy) an existing db",
             dbsize =>
             "size of the default datafile, e.g. dbsize=1g",

             # hidden definitions (use leading underscore)
             _QUIETWHISPER => 
             "quiet whisper state"
             );
        my %defs2 = %{$args{defs}};

        for my $key (keys(%legitdefs))
        {
            if (exists($defs2{$key}))
            {
                $dictargs{$key} = $defs2{$key};

                if ($key =~ m/QUIETWHISPER/)
                {
                    whisper "quietwhisper is  $Genezzo::Util::QUIETWHISPER";
                    $Genezzo::Util::QUIETWHISPER = $defs2{$key};
                    whisper "set quietwhisper to $Genezzo::Util::QUIETWHISPER";
                }
                delete $defs2{$key};
            }
        }
        
        if (scalar(keys(%defs2)))
        {
            my $msg = "unknown definitions for database initialization:\n";
            while (my ($kk, $vv) = each (%defs2))
            {
                $msg .=  "\t" .  $kk .  "=" . $vv ."\n";
            }
            $msg .= "\nlegal values are:\n";
            while ( my ($kk, $vv) = each (%legitdefs))
            {
                $msg .= "  $kk - $vv\n"
                    if ($kk !~ /^\_/); # hide defs with leading underscores
            }          
            $msg .= "\n";  

            my %earg = ( msg => $msg, severity => 'fatal');

            &$GZERR(%earg)
                if (defined($GZERR));

            return 0;
        }

    }

    if (exists($args{GZERR})) # pass the error reporting routine
    {
        $dictargs{GZERR} = $args{GZERR};
    }

    $self->{dbh_ctx} = {}; # database handle context

    $self->{dictobj} = Genezzo::Dict->new(gnz_home => $self->{gnz_home}, 
                                          init_db => $init_db, %dictargs);
    return 0
        unless (defined($self->{dictobj}));

    return 1;
}

sub _clearerror
{
    my $self = shift;
    $self->{errstr} = undef;
    $self->{err}    = undef;
}

# DBI-style connect
#
# Arguments:
# 
# gnz_home   (required): genezzo home directory
# username   (required, but ignored): user name
# password   (required, but ignored): password
# attributes (optional): hash of attributes
#
# example:
# my $dbh = Genezzo::GenDBI->connect($gnz_home, 
#                                    "NOUSER", "NOPASSWORD",
#                                    {GZERR => $GZERR,
#                                     PrintError => 1});
#
sub connect # DBI
{
    my $invocant = shift;
    my $class = ref($invocant) || $invocant ; 
    my $self = { };

    my ($gnz_home, $user, $passwd, $attr) = @_;

    my %optional; # some optional values for _init args...
    
    $self->{PrintError} = 1;
    $self->{RaiseError} = 0;

    if (defined($attr) && (ref($attr) eq 'HASH'))
    {
        # standard DBI-style PrintError, RaiseError
        if (exists($attr->{PrintError}))
        {
            $self->{PrintError} = $attr->{PrintError};
        }
        if (exists($attr->{RaiseError}))
        {
            $self->{RaiseError} = $attr->{RaiseError};
        }
        # Non-standard GZERR argument to supply error message handler
        if ((exists($attr->{GZERR}))
                && (defined($attr->{GZERR})))
        {
            $optional{GZERR} = $attr->{GZERR};
        }
    }

    my $i_gzerr  = sub {
        my %args = (@_);

        return 
            unless (exists($args{msg}));
        
        my $warn = 0;
        if (exists($args{severity}))
        {
            my $sev = uc($args{severity});
            $sev = 'WARNING'
                if ($sev =~ m/warn/i);
            
            # don't print 'INFO' prefix
            if ($args{severity} !~ m/info/i)
            {
#                printf ("%s: ", $sev);
#                $warn = 1;
            }
            else
            {
#                printf ("%s: ", $sev);
#                print $args{msg}, "\n";
                return;
            }
        };

        my $l_errstr = $args{msg} . "\n";
        $self->{errstr} = $l_errstr;

        warn $l_errstr
            if $self->{PrintError};
        die $l_errstr
            if $self->{RaiseError};

    };

    # if no GZERR was supplied, use the dbi-style handler declared above
    # with the appropriate printError, raiseError settings.
    $optional{GZERR} = $i_gzerr
        unless ((exists($optional{GZERR}))
                && (defined($optional{GZERR})));

    my %nargs = (%optional,
                exe => $0,
                gnz_home => $gnz_home,
                user => $user,
                password => $passwd);

    if ((exists($nargs{GZERR}))
        && (defined($nargs{GZERR}))
        && (length($nargs{GZERR})))
    {
        $self->{GZERR} = $nargs{GZERR};
        my $err_cb     = $self->{GZERR};
        # capture all standard error messages
        $Genezzo::Util::UTIL_EPRINT = 
            sub {
                &$err_cb(self     => $self,
                         severity => 'error',
                         msg      => @_); };
        
        $Genezzo::Util::WHISPER_PRINT = 
            sub {
                &$err_cb(self     => $self,
#                         severity => 'error',
                         msg      => @_); };
    }

    return undef
        unless (_init($self,%nargs));


    return bless $self, $class;

} # end connect

sub new
{
    my $invocant = shift;
    my $class = ref($invocant) || $invocant ; 
    my $self = { };

    my %args = (@_);

    if ((exists($args{GZERR}))
        && (defined($args{GZERR}))
        && (length($args{GZERR})))
    {
        # NOTE: don't supply our GZERR here - will get
        # recursive failure...
        $self->{GZERR} = $args{GZERR};
        my $err_cb     = $self->{GZERR};
        # capture all standard error messages
        $Genezzo::Util::UTIL_EPRINT = 
            sub {
                &$err_cb(self     => $self,
                         severity => 'error',
                         msg      => @_); };
        
        $Genezzo::Util::WHISPER_PRINT = 
            sub {
                &$err_cb(self     => $self,
#                         severity => 'error',
                         msg      => @_); };
    }

    return undef
        unless (_init($self,%args));

    return bless $self, $class;

} # end new

sub Kgnz_Rem
{
    my $self = shift;
    return 1;
}

sub Kgnz_Quit
{
    my $self = shift;
    my %earg = (self => $self, msg => "quitting...\n", severity => 'info');

    &$GZERR(%earg)
        if (defined($GZERR));

    exit ;

    return 1;
	
} # end Kgnz_Quit

sub Kgnz_Reload
{
    my $self = shift;

    if (exists($self->{caller}))
    {
        my $msg  = $self->{caller} . "\n";
        my %earg = (self => $self, msg => $msg, severity => 'info');

        &$GZERR(%earg)
            if (defined($GZERR));
        
        # need to add arg list here ... 
        exec $self->{caller}  ;
    }
#    return ;
}

sub Kgnz_Dump
{
    my $self = shift;
    my $dictobj = $self->{dictobj};
    return $dictobj->DictDump (@_);

}

sub Kgnz_AddFile
{
#    greet @_ ;
    my $self = shift;
    my $dictobj = $self->{dictobj};
    {
        my $goodargs = 1;
        my $gothelp  = 0;
        my %legitdefs = 
            (
             filesize => 
             "size of a database file in bytes, e.g. filesize=10M\n\t\t(default - double previous allocation)",
             filename =>
             "name of file (default - system-generated)",
             tsname =>
             "\tname of associated tablespace (default SYSTEM)",
             increase_by =>
             "size in bytes or percentage increase, e.g. increase_by=1M\n\t\tor increase_by=50% (default zero - file size is fixed)"
             );
        my %nargs;

        $nargs{dbh_ctx} = $self->{dbh_ctx};

        for my $argval (@_)
        {
            if ($argval =~ m/^help$/i)
            {
                my $bigMsg;
                ($bigMsg = <<EOF_Msg) =~ s/^\#//gm;
#
# AddFile Help - addfile takes a list of name=value arguments
# with no spaces around the equal sign, and no commas between arguments
# e.g: addfile filename=test.dbf filesize=22M
#
# If no arguments are specified addfile will create a new datafile
# double the size of the previous one.
#
EOF_Msg
                my %earg = (self => $self, msg => $bigMsg, severity => 'warn');

                &$GZERR(%earg)
                    if (defined($GZERR));
                
                $gothelp  = 1;
                $goodargs = 0;
                last;
            }

            if ($argval =~ m/=/)
            {
                my @foo = split('=',$argval, 2);
                if ((2 == scalar(@foo))
                    && (defined($foo[0]))
                    && (exists($legitdefs{$foo[0]})))
                {
                    $nargs{$foo[0]} = $foo[1];
                }
                else
                {
                    my $msg = "invalid argument: $argval\n";
                    my %earg = (self => $self, msg => $msg, 
                                severity => 'warn');
                    
                    &$GZERR(%earg)
                        if (defined($GZERR));

                    $goodargs = 0;
                }
            }
            else
            {
                my $msg = "invalid argument: $argval\n";
                my %earg = (self => $self, msg => $msg,
                            severity => 'warn');
                    
                &$GZERR(%earg)
                    if (defined($GZERR));
                $goodargs = 0;
            }
        } # end for
        unless ($goodargs)
        {
            my $msg = "valid args are:\n";
            while (my ($kk, $vv) = each (%legitdefs))
            {
                $msg .= $kk . ":\t" . $vv ."\n";
            }
            $msg .= "type: \"addfile help\" for more information\n"
                unless ($gothelp);

            my %earg = (self => $self, msg => $msg,
                        severity => 'warn');
            
            &$GZERR(%earg)
                if (defined($GZERR));

            return 0;
        }

        return ($dictobj->DictAddFile (%nargs));
    }

    return 0;
        
}

sub Kgnz_Describe
{
    my $self = shift;
    my $dictobj = $self->{dictobj};

  L_ParseDescribe:
    {
	last if (@_ < 1);

	my $tablename = shift @_ ;

	my @params = @_ ;

        my $allcols = $dictobj->DictTableGetCols (tname => $tablename);

        return undef
            unless (defined($allcols));

        my @outi;
        while (my ($kk, $vv) = each (%{$allcols}))
        {
            my ($colidx, $dtype) = @{$vv};

            $outi[$colidx] = "$kk : $dtype\n";
        }
        my $bigMsg = "";
        for my $ii (@outi)
        {
            $bigMsg .= $ii
                if (defined($ii));
        }
        my %earg = (self => $self, msg => $bigMsg, severity => 'info');

        &$GZERR(%earg)
            if (defined($GZERR));

        return 1;

    }
    return 0;

} # end describe

sub Kgnz_CIdx
{
    my $self = shift;
  L_ParseCreate:
    {
	last if (@_ < 3);

	my $indexname = shift @_ ;
	my $tablename = shift @_ ;

        my $msg = "Create Index : $indexname on $tablename \n";

        my %earg = (self => $self, msg => $msg, severity => 'info');
        
        &$GZERR(%earg)
            if (defined($GZERR));

	my @params = @_ ;

        unless (scalar(@params))
        {
            $msg = "invalid column list for table $tablename\n";
            %earg = (self => $self, msg => $msg, severity => 'warn');
        
            &$GZERR(%earg)
                if (defined($GZERR));

            return 0;
        }
        my $dictobj = $self->{dictobj};
        return ($dictobj->DictIndexCreate (tname      => $tablename,
                                           index_name => $indexname,
                                           cols       => \@params,
                                           tablespace => "SYSTEM",
                                           itype      => "nonunique",
                                           dbh_ctx    => $self->{dbh_ctx}
                                           ));

    }
    return 0;
}

sub Kgnz_CT
{
    my $self = shift;
  L_ParseCreate:
    {
	last if (@_ < 1);

	my $tablename = shift @_ ;

	my @params = @_ ;

        unless (scalar(@params))
        {
            my $msg = "invalid column list for table $tablename\n";
            my %earg = (self => $self, msg => $msg,
                        severity => 'warn');
            
            &$GZERR(%earg)
                if (defined($GZERR));
            return 0;
        }

        my @coldefarr = ();
        
        my $colidx = 0;

        my $tabtype = "TABLE";

        my $msg = "Create Table : $tablename \n";

        # XXX XXX: quick hack for index-organized table support
        if ($params[0] =~ m/^index/i)
        {
            $msg .= "with unique index option\n";
            $tabtype = "IDXTAB";
            shift @params
        }
        my %earg = (self => $self, msg => $msg,
                    severity => 'info');
                    
        &$GZERR(%earg)
            if (defined($GZERR));

      L_coldataloop:
        foreach my $token (@params)
        {
            unless ($token =~ m/=/)
            {
                $msg = "invalid column specifier ($token) for table $tablename\n";
                %earg = (self => $self, msg => $msg,
                         severity => 'warn');
                
                &$GZERR(%earg)
                    if (defined($GZERR));

                return 0;
            }

            my ($colname, $dtype) = split('=',$token) ;
            
            $coldefarr[$colidx++] = {colname => $colname,
                                     datatype => $dtype};
        }
        
#            greet %coldatatype;

        my %args =
            (op1 => "create",
             op2 => "table",
             createtabargs => 
             {
                 tabname => $tablename,
                 tabdef  => 
                 {
                     coldefarr => \@coldefarr
                 },
                 dbstore    => "flat1",
                 tablespace => "SYSTEM",
                 object_type  => $tabtype
             }
             );
        return $self->Kgnz_Create(%args);

    }
    return 0;

} # end CT

sub Kgnz_Create
{
    my $self = shift;
    my $dictobj = $self->{dictobj};
    my %args = (
		@_);
#		op1, op2

    my $bVerbose = 1;

    my %createdispatch = 
	qw(
	   table  tablething
	   tabdef tadefthing
	   );

#    greet @_ ;

  L_ParseCreate:
    {
	my $createkeyword = $args{op2};

        my ($msg,%earg);

	unless (exists($createdispatch{lc($createkeyword)}))
	{
	    $msg = "could not parse: \n" ;
            my $b = \%args;
	    $msg .= Data::Dumper->Dump([$b], [qw(*b )]); 
            %earg = (self => $self, msg => $msg,
                     severity => 'warn');
                    
            &$GZERR(%earg)
                if (defined($GZERR));

	    last   L_ParseCreate; 
	}

        unless (exists($args{createtabargs}))
        {
            $msg = "no table name \n" ;
            %earg = (self => $self, msg => $msg,
                     severity => 'warn');
                    
            &$GZERR(%earg)
                if (defined($GZERR));

            last L_ParseCreate;
        }

        my $tabargs = $args{createtabargs};

        unless (exists($tabargs->{tabname}))
        {
            $msg = "no table name \n" ;
            %earg = (self => $self, msg => $msg,
                     severity => 'warn');
                    
            &$GZERR(%earg)
                if (defined($GZERR));

            last L_ParseCreate;
        }

	my $tablename = $tabargs->{tabname} ;
	my $tabdefn   = $tabargs->{tabdef} ;
        my $tabtype   = $tabargs->{object_type} || "TABLE";

        unless ($dictobj->DictTableExists (tname => $tablename,
                                           silent_exists => 0,
                                           silent_notexists => 1 ))
        {

            my %legaldtypes = 
                qw(
                   c      charthing
                   char   charthing
                   n       numthing
                   num     numthing
                   );

            # NB: get keys in insertion order
#            use Tie::IxHash ;

            my %coldatatype = ();

#            tie %coldatatype, "Tie::IxHash"; 

            my $colidx = 1;

            if ($bVerbose )
            {

                $msg = "tablename : $tablename\n" ;
                %earg = (self => $self, msg => $msg,
                         severity => 'info');
                    
                &$GZERR(%earg)
                    if (defined($GZERR));
            }

          L_coldataloop:
            foreach my $token (@{ $tabdefn->{coldefarr} })
            {
                my $colname = $token->{colname};
                my $dtype   = $token->{datatype};

                unless (exists($legaldtypes{lc($dtype)}))
                {
                    $msg = "illegal datatype: $dtype \n" ;
                    $msg .= "$tablename : " . Dumper($token) . "\n";
                    %earg = (self => $self, msg => $msg,
                             severity => 'warn');
                    
                    &$GZERR(%earg)
                        if (defined($GZERR));

                    last   L_ParseCreate; 
                }
                if ($bVerbose)
                {
                    my $extra = "";
                    $extra = '(primary key)' # XXX XXX
                        if (($tabtype eq "IDXTAB") && (1 == $colidx));

                    $msg = "\tcolumn $colname : $dtype $extra\n" ;
                    %earg = (self => $self, msg => $msg,
                             severity => 'info');
                    
                    &$GZERR(%earg)
                        if (defined($GZERR));
                }

                $coldatatype{$colname} = [$colidx, $dtype];
                $colidx++;

            }

#            greet %coldatatype;

            # create hash ref
            
            return ($dictobj->DictTableCreate (tname       => $tablename,
                                               tabdef      => \%coldatatype,
                                               tablespace  => "SYSTEM",
                                               object_type => $tabtype,
                                               dbh_ctx     => $self->{dbh_ctx}
                                               ));


        }
    }
    return 0;
        
}

sub Kgnz_Drop
{
#    greet @_ ;
    my $self = shift;
    my $dictobj = $self->{dictobj};
    {
        last if (@_ < 1);
        my $stat;

        for my $tablename ( @_ )
        {
            next # optional "table" keyword... [not SQL standard]
                if ($tablename =~ m/^table$/i);

            $stat = $dictobj->DictTableDrop (tname   => $tablename,
                                             dbh_ctx => $self->{dbh_ctx}
                                             );

            last 
                unless ($stat);
        }

        return $stat;
    }

    return undef;
        
}

sub Kgnz_Spool
{
#   greet @_;    
    my $self = shift;
    {
        last if (@_ < 1);

        my $outfile = shift @_ ;
        my @params = @_ ;

        if (uc($outfile) eq "OFF")
        {
            close  (NEWOUT);
            select (STDOUT) ; 
            last;
        }

        close (NEWOUT);
        open (NEWOUT, "| tee $outfile ") 
            or die "Could not tee open $outfile for writing : $! \n";

        $| = 1; # force flush
	select (NEWOUT);
	$| = 1;
    }

    return 1;
	
}

sub Kgnz_Commit
{
    my $self = shift;
    my $dictobj = $self->{dictobj};
    my %args = (
		@_);

#    greet @_ ; 

    return ($dictobj->DictSave(dbh_ctx => $self->{dbh_ctx}));
	
}

sub Kgnz_Rollback
{
    my $self = shift;
    my $dictobj = $self->{dictobj};
    my %args = (
		@_);

#    greet @_ ; 

    return ($dictobj->DictRollback(dbh_ctx => $self->{dbh_ctx}));
	
}

sub PrintVersionString
{
    my $self = shift;
    my $msg = "\n\nGenezzo Version $VERSION - $RELSTATUS $RELDATE  (www.genezzo.com)\n";
    $msg .= "Copyright (c) 2003, 2004, 2005 Jeffrey I Cohen.  All rights reserved.\n";
    $msg .= "\nType \"SHOW\" to obtain license information.\n\n";
    my %earg = (self => $self, msg => $msg,
             severity => 'info');
                    
    &$GZERR(%earg)
        if (defined($GZERR));

}

sub Kgnz_Show
{
    my $self = shift;
    my $dictobj = $self->{dictobj};

    my $msg = "";
    my $severity = 'info';

    my %legitdefs = 
        (version => "Genezzo version information",
         license => "Genezzo license and warranty",
         help    =>  "this message"
         );

    my $showhelp = !(scalar(@_));
    for my $argval (@_)
    {
        if ($argval =~ m/license/i)
        {
            $self->PrintLicense();
        }
        elsif ($argval =~ m/version/i)
        {
            $self->PrintVersionString();
        }
        elsif ($argval =~ m/help/i)
        {
            $showhelp = 1;
        }
        else
        {
            $showhelp = 1;
            $msg = "invalid SHOW argument ($argval)\n";
            $severity = 'warn';
        }
    }
    if ($showhelp)
    {
        $msg .= "\nlegal values are:\n";
        while ( my ($kk, $vv) = each (%legitdefs))
        {
            $msg .= "  show $kk - $vv\n";
        }          
        my %earg = (self => $self, msg => $msg,
                    severity => $severity);
                    
        &$GZERR(%earg)
            if (defined($GZERR));
    }

    return 1;
}

sub Kgnz_Startup
{
    my $self = shift;
    my $dictobj = $self->{dictobj};
    my %args = (
		@_);

    greet @_ ; 
    $args{dbh_ctx} = $self->{dbh_ctx};
    return $dictobj->DictStartup(@_);
}

sub Kgnz_Shutdown
{
    my $self = shift;
    my $dictobj = $self->{dictobj};
    my %args = (
		@_);

    greet @_ ; 
    $args{dbh_ctx} = $self->{dbh_ctx};
    return $dictobj->DictShutdown(@_);
}

sub Kgnz_Password
{
    my $self = shift;
    my $dictobj = $self->{dictobj};
    my %args = (
		@_);

    greet @_ ; 

    my ($uname, $cryptpwd) = (getpwuid($<))[0,1];
    my $plainword;

    # XXX XXX : looks like this getpwuid returns the crypt from the
    # shadow file - an 'x'

# XXX XXX : need term::readkey    

    system "stty -echo";
    print "Password: ";
    chomp($plainword = <STDIN>);
    print "\n";
    system "stty echo";
    
    if (crypt($plainword, $cryptpwd) ne $cryptpwd) {
        print "sorry!\n";
    } else {
        print "ok\n";
    }


    return 1;
}

sub Kgnz_Delete
{
#    greet @_ ; 
    my $self = shift;
    my $dictobj = $self->{dictobj};
  L_ParseDelete:
    {
	last if (@_ < 2);

	my $tablename = shift @_ ;
	my @params = @_ ;
#        greet @params;

        my ($msg, %earg);
        my $severity = 'info';

	last unless $dictobj->DictTableExists(tname => $tablename);

        my $rowcount = 0;

        $msg = "";
        foreach my $rid (@params)
        {
            unless 
                ($dictobj->RowDelete (tname   => $tablename, 
                                      rid     => $rid,
                                      dbh_ctx => $self->{dbh_ctx}
                                      )
                 )
                {
                    $msg = "failed to delete row $rid : \n";
                    $severity = 'warn';

                    last;
                }
            
            $rowcount++;
        }
        my $rowthing = ((1 == $rowcount) ? "row" : "rows");
        $msg .= "deleted $rowcount $rowthing from table $tablename.\n";
        %earg = (self => $self, msg => $msg,
                 severity => $severity);
                    
        &$GZERR(%earg)
            if (defined($GZERR));

        return $rowcount;
    }

    return undef;
	
} # end kgnz_delete

sub Kgnz_Insert
{
#    greet @_ ; 
    my $self = shift;
    my $dictobj = $self->{dictobj};

    return undef
        if (@_ < 2);

    my $tablename = shift @_ ;

    my $collist = [];

    return $self->Kgnz_Insert2($tablename, $collist, @_);
}

sub Kgnz_Insert2
{
#    greet @_ ; 
    my $self = shift;
    my $dictobj = $self->{dictobj};
  L_ParseInsert:
    {
	last if (@_ < 3);

	my $tablename = shift @_ ;
        my $collist   = shift @_ ;
	my @params = @_ ;

        my ($msg, %earg);
        my $severity = 'info';

	last unless $dictobj->DictTableExists(tname => $tablename);

        my $rowcount = 0;
        my @rowarr = ();

        # take the scalar of keys for number of items in hash
        my $numitems 
            = scalar(keys(%{$dictobj->DictTableGetCols (tname => 
                                                        $tablename)}));

        if (scalar(@{$collist}) > $numitems)
        {
            $msg = "too many columns";
            %earg = (self => $self, msg => $msg,
                     severity => 'warn');
                    
            &$GZERR(%earg)
                if (defined($GZERR));

            return undef;
        }

        $msg = "";
        unless (scalar(@{$collist}))
        {
            while (@rowarr = splice (@params, 0, $numitems))
            {

                unless ($dictobj->RowInsert (tname   => $tablename, 
                                             rowval  => \@rowarr,
                                             dbh_ctx => $self->{dbh_ctx}
                                             )
                        )
                {
                    my $rr = $rowcount + 1;
                    $msg = "Failed to insert row $rr in table $tablename\n";
                    $severity = 'warn';
                    
                    last;
                }
            
                $rowcount++;
                @rowarr = ();
            }
            my $rowthing = ((1 == $rowcount) ? "row" : "rows");
            $msg .= "inserted $rowcount $rowthing into table $tablename.\n";
            %earg = (self => $self, msg => $msg,
                     severity => $severity);
            
            &$GZERR(%earg)
                if (defined($GZERR));

            return $rowcount
        } # end unless

        my @match;
        my %colh; # check for dups

        for my $colname (@{$collist})
        {
            my $colnum;

            unless ($colnum
                    = $dictobj->DictTableColExists (tname => $tablename,
                                                    colname => $colname))
            {
                if ($colname =~ m/(?i)^(rid|rownum)$/)
                {
                    $colname = uc $colname;
                    $msg = "cannot update ($colname) pseudo column";
                }
                else
                {
                    $msg = "no such column ($colname) in $tablename";
                }
                %earg = (self => $self, msg => $msg,
                         severity => 'warn');
                    
                &$GZERR(%earg)
                    if (defined($GZERR));

                return undef;
            }

            if (exists($colh{$colnum}))
            {
               $msg = "column ($colname) specified more than once";
               %earg = (self => $self, msg => $msg,
                        severity => 'warn');
                    
               &$GZERR(%earg)
                   if (defined($GZERR));

                return undef;
            }
            $colh{$colnum} = 1;
            push @match, ($colnum - 1);
        } # end for all columns

        $msg = "";
        while (scalar(@params))
        {
          L_mfor:
            for my $mm (@match)
            {
                $rowarr[$mm] = shift @params;
                last L_mfor
                    unless scalar(@params);
            }
            unless ($dictobj->RowInsert (tname   => $tablename, 
                                         rowval  => \@rowarr,
                                         dbh_ctx => $self->{dbh_ctx}
                                         )
                    )
            {
                my $rr = $rowcount + 1;
                $msg = "Failed to insert row $rr in table $tablename\n";
                $severity = 'warn';
                    
                &$GZERR(%earg)
                    if (defined($GZERR));

                last;
            }
            
            $rowcount++;
            @rowarr = ();

            # NOTE: don't bother generating null trailing columns --
            # unpack will create an array of existing columns, and
            # trailing columns will instantiate as null if
            # referenced...
#            $#rowarr = $numitems; # map for all columns

        } # end while param
        my $rowthing = ((1 == $rowcount) ? "row" : "rows");
        $msg = "inserted $rowcount $rowthing into table $tablename.\n";
        %earg = (self => $self, msg => $msg,
                 severity => $severity);
                    
        &$GZERR(%earg)
            if (defined($GZERR));

        return $rowcount;

    }

    return undef;
	
} # end parseinsert

sub Kgnz_Update
{
#    greet @_ ; 
    my $self = shift;
    my $dictobj = $self->{dictobj};
  L_ParseUpdate:
    {
	last if (@_ < 2);

	my $tablename = shift @_ ;
        my $rid = shift @_ ;
	my @params = @_ ;

        my ($msg, %earg);
        my $severity = 'info';

	last unless $dictobj->DictTableExists(tname => $tablename);

	# take the scalar of keys for number of items in hash
	my $numitems 
            = scalar(keys(%{$dictobj->DictTableGetCols (tname => $tablename)}));

        my $rowcount = 0;

        # Note: ignore extra columns -- don't loop like an insert
        my @rowarr = splice (@params, 0, $numitems);

        $msg = "";
        {
            unless 
                ($dictobj->RowUpdate (tname   => $tablename, 
                                      rid     => $rid,
                                      rowval  => \@rowarr,
                                      dbh_ctx => $self->{dbh_ctx}
                                      )
                 )
                {
                    $msg = "failed to update row $rid : \n";
                    $severity = 'warn';
                    goto L_up1; # last
                }
            
            $rowcount++;
            @rowarr = ();
          L_up1:

        }
        my $rowthing = ((1 == $rowcount) ? "row" : "rows");
        $msg = "updated $rowcount $rowthing in table $tablename.\n";
        %earg = (self => $self, msg => $msg,
                 severity => $severity);
                    
        &$GZERR(%earg)
            if (defined($GZERR));

        return $rowcount;
    }

    return undef;
	
} # end kgnz_update


sub SQLSelect
{
    my $self = shift;
    my @ggg = $self->SQLSelectPrepare(@_);

    return undef
        unless (scalar(@ggg));

    my @hhh = $self->SelectExecute(@ggg);

    return undef
        unless (scalar(@hhh));

    return $self->SelectPrint(@hhh);
}

sub SQLSelectPrepare
{
    my $self = shift;    

    my $sqltxt = $self->{current_line};

    my ($sql_cmd, $pretty, $badparse) =
        $self->{feeble}->Parseall($sqltxt);

    return undef
        if ($badparse);

    return $self->_SQLselprep($sql_cmd);
}

sub _SQLselprep
{
    my ($self, $sql_cmd) = @_;
    my @colpairs;

    # XXX: no joins supported yet!

    unless (exists($sql_cmd->{from_list}->[0]->{val}))
    {
        greet $sql_cmd->{from_list};
        return undef;
    }

    my $tablename = $sql_cmd->{from_list}->[0]->{val};

    foreach my $i (@{$sql_cmd->{select_list}})
    {
        push @colpairs, [$i->{val}, $i->{name}];
    }
    my $where;
    if (exists($sql_cmd->{where_list}))
    {
        $where = $sql_cmd->{where_list};
#        greet $where;
    }

    return ($self->SelectPrepare(tablename => $tablename, 
                                 colpairs  => \@colpairs,
                                 where     => $where));
}

sub SQLWhere
{
    my $self = shift;    
    my $dictobj = $self->{dictobj};
    my %args = (@_);

    my $tablename = $args{tablename};
    my $where = $args{where};

#    greet $where;

    # transform standard sql relational operators to Perl-style,
    # distinguishing numeric and character comparisons
    my $relop = 
    {
        '==' => { "n" => "==",  "c" => "eq"},
        '='  => { "n" => "==",  "c" => "eq"},
        '<>' => { "n" => "!=",  "c" => "ne"},
        '!=' => { "n" => "!=",  "c" => "ne"},
        '>'  => { "n" => ">",   "c" => "gt"},
        '<'  => { "n" => "<",   "c" => "lt"},
        '>=' => { "n" => ">=",  "c" => "ge"},
        '<=' => { "n" => "<=",  "c" => "le"},

        '<=>' => { "n" => "<=>",  "c" => "cmp"}
    };

    # XXX XXX: filter will complain about "uninitialized" strings

    my $filterstring = '
   $filter = sub {

        no warnings qw(uninitialized); # shut off null string warnings

        my ($tabdef, $rid, $outarr) = @_;
        return 1
            if (defined($outarr) &&
                scalar(@{$outarr}) &&
                ( ';


    my $where_text = "";

    my @filtary; # build an array of filter expressions

    my $prevcoltype;

    my $maxw = scalar((@{$where}));

    my $badparse = 0;
    my $doskip = -1;  # handle IS [NOT] NULL token lookahead

    my $AndPurity = 1;    # WHERE clauses of ANDed predicates may
    my @AndTokens;        # be suitable for index lookups, but ORs
    my $OrBars = '\|\|';  # can be a problem.  Test for "And Purity".

    my ($msg, %earg);

    for my $ii (0..($maxw - 1))
    {
        if ($doskip >= 0)
        {
            if ($ii == $doskip)
            {
                $doskip = -1;
            }
            next;
        }

        my $val = $where->[$ii];
        my $token = $val->{val};

        unless ($val->{type} eq 'IDENTIFIER')
        {
            if ($val->{type} =~ m/RESERVED/) 
            {
                if ($token =~ m/^IS$/i)
                {
                    $doskip = $ii;
                    $where_text .= "$token ";

                    my $isnot = -1;

                  L_isnull:
                    while (1)
                    {
                        my $v2 = $where->[$doskip+1];

                        # peek ahead to find [NOT] NULL --
                        # fail if these tokens missing
                        # of if no previous value to pop from filtary
                        if (!defined($v2) || !scalar(@filtary))
                        {
                            $msg = "could not parse IS NULL\n";
                            $badparse = 1;
                            last L_isnull;
                        }
                        else
                        {                        

                            $where_text .= "$v2->{val} ";

                            if ($v2->{val} =~ m/^NOT$/i)
                            {
                                $isnot *= -1;
                                $doskip++;
                                next L_isnull;
                            }
                            elsif ($v2->{val} =~ m/^NULL$/i)
                            {
                                $doskip++;
                                my $gg = pop @filtary;
                                my $bang = ($isnot < 0) ? '!' : '';
                                push @filtary, 
                                     $bang . '(defined(' . $gg . '))';

                                $AndPurity = ($isnot > 0);

                                last L_isnull;
                            }
                            else
                            {
                                $msg = "could not parse IS NULL\n";
                                $badparse = 1;
                                last L_isnull;
                            }
                        } 
                    } # end while isnull

                    # go back and SKIP all the IS [NOT...] NULL tokens
                    next;
                } # end is

            } # end reserved

            if (defined($prevcoltype)
                && exists($relop->{$token})
                && exists($relop->{$token}->{$prevcoltype}))
            {
                my $relop1 = $relop->{$token}->{$prevcoltype};
                
                my $tok_expr = '(eq|==|<|>|lt|gt|le|ge|<=|>=)';
                if ($relop1 =~ m/^$tok_expr$/)
                {
                    $AndTokens[$ii] = {op => $relop1};
                }

                push @filtary, $relop1;
                $where_text .= "$token ";
            }
            else
            {
                if (($token =~ m/or/i) || ($token =~ m/$OrBars/))
                {
                    $AndPurity = 0; # found "ORs"
                }
                if ($ii)
                {
                    my $prev = $AndTokens[$ii-1];

                    # check for a leading "column = " operator...
                    if (defined($prev) &&
                        exists($prev->{op}))
                    {
                        $AndTokens[$ii] = {literal => $token};
                    }
                }

                push @filtary, $token;
                $where_text .= "$token ";
            }
            if ($val->{type} =~ m/QUOTESTR/) # quoted string is char
            {
                $prevcoltype = "c";
            }
            if ($val->{type} =~ m/NUMERIC/)
            {
                $prevcoltype = "n";
            }

            next;
        }

        my ($colnum, $coltype)
            = $dictobj->DictTableColExists (tname => $tablename,
                                            colname => $token,
                                            silent_notexists => 1);
        if ($colnum)
        {
            $AndTokens[$ii] = {col => ($colnum - 1)};

            push @filtary, '$outarr->[' . ($colnum - 1) . ']';
            $prevcoltype = $coltype;
            $where_text .= "$token ";
        }
        else
        {
            if ($token =~ m/^rid$/i) # rid pseudocolumn
            {
                push @filtary, '$rid';
                $prevcoltype = "c";
                $where_text .= "$token ";
            }
            else
            {
                if (($token =~ m/or/i) || ($token =~ m/$OrBars/))
                {
                    $AndPurity = 0; # found "ORs"
                }

                push @filtary, $token;
                $where_text .= "$token ";
            }
        }
    } # end for
    
    for my $f1 (@filtary)
    {
        $filterstring .= "$f1 ";
    }

    $filterstring .= "));};";

#    greet $filterstring;
#    greet "pure", $AndPurity, @AndTokens;
    @AndTokens = ()
        unless ($AndPurity);

    my $filter;     # the anonymous subroutine which is the 
                    # result of eval of filterstring

    my $status;

    if ($badparse)
    {
        %earg = (self => $self, msg => $msg,
                 severity => 'warn');
                    
        &$GZERR(%earg)
            if (defined($GZERR));
    }
    else
    {
        $status = eval " $filterstring ";
    }

    unless (defined($status))
    {
        $msg = "";
#        warn $@ if $@;
        $msg .= $@ 
            if $@;
        $msg .= "\nbad filter:\n";
        $msg .= $filterstring . "\n";
        $msg .= "\nWHERE clause:\tWHERE " . $where_text . "\n";
        %earg = (self => $self, msg => $msg,
                 severity => 'warn');
                    
        &$GZERR(%earg)
            if (defined($GZERR));

        return undef;
    }
#    greet $filter; 

    my %hh = (idxfilter => \@AndTokens);
    $hh{filter} = $filter
        if (defined($filter));
    $hh{where_text} = $where_text;
    $hh{filter_text} = $filterstring;
#    greet %hh;
    return \%hh;
} # end SQLWhere

sub SQLCreate
{
    my $self = shift;    
    my $dictobj = $self->{dictobj};
    my $sqltxt = $self->{current_line};

    my ($sql_cmd, $pretty, $badparse) =
        $self->{feeble}->Parseall($sqltxt);

    return undef
        if ($badparse);

#    greet $sql_cmd;

    # lists of equivalent types for char and numeric data
    # XXX XXX: nchar?
    my @chartype1 = qw(
                       char character varchar varchar2 long
                       );
    push @chartype1, ("character varying", "char varying", "long varchar");
    my @numtype1  = qw(
                       numeric decimal dec integer int smallint 
                       float real
                       number
                       );
    push @numtype1, ("double precision");
                       
    my $char_regex = '(?i)^(' . (join '|', @chartype1) . ')$';
    my $num_regex  = '(?i)^(' . (join '|', @numtype1)  . ')$';

    if (exists($sql_cmd->{operation}))
    {
        if (($sql_cmd->{operation} =~ m/^table$/i)
            && exists($sql_cmd->{cr_table_list}))
        {
            my @outi;

            return undef
                unless (exists($sql_cmd->{cr_table_list}->{tablename}));

            push @outi, $sql_cmd->{cr_table_list}->{tablename};
            
            if (exists($sql_cmd->{cr_table_list}->{columns}))
            {
                for my $col (@{$sql_cmd->{cr_table_list}->{columns}})
                {
                    my $colname = $col->{name};
                    my $coltype = $col->{type};
                    
                    # convert to bogus internal number/char types
                    $coltype = "c"
                        if ($coltype =~ m/$char_regex/);

                    $coltype = "n"
                        if ($coltype =~ m/$num_regex/);

                    push @outi, "$colname=$coltype";
                }

            }
            return $self->Kgnz_CT(@outi);
        }
        elsif (($sql_cmd->{operation} =~ m/^index$/i)
            && exists($sql_cmd->{cr_index_list}))
        {
            my @outi;

            return undef
                unless (exists($sql_cmd->{cr_index_list}->{indexname}));
            return undef
                unless (exists($sql_cmd->{cr_index_list}->{tablename}));

            push @outi, $sql_cmd->{cr_index_list}->{indexname};
            push @outi, $sql_cmd->{cr_index_list}->{tablename};
            
            if (exists($sql_cmd->{cr_index_list}->{columns}))
            {
                for my $col (@{$sql_cmd->{cr_index_list}->{columns}})
                {
                    my $colname = $col->{name};

                    push @outi, $colname;
                }
            }

            return $self->Kgnz_CIdx(@outi);
        }

    }

} # end SQLCreate

sub SQLAlter
{
    my $self = shift;    
    my $dictobj = $self->{dictobj};
    my $sqltxt = $self->{current_line};

    my ($sql_cmd, $pretty, $badparse) =
        $self->{feeble}->Parseall($sqltxt);

    return undef
        if ($badparse);

#    greet $sql_cmd;

    my $tablename;
    my ($msg, %earg);

    if (exists($sql_cmd->{operation}))
    {
        if (($sql_cmd->{operation} =~ m/^table$/i)
            && exists($sql_cmd->{alt_table_list}))
        {
            my @outi;

            return undef
                unless (exists($sql_cmd->{alt_table_list}->{tablename}));

            $tablename = $sql_cmd->{alt_table_list}->{tablename};

            return undef
                unless $dictobj->DictTableExists (tname => $tablename);

#            push @outi, $sql_cmd->{alt_table_list}->{tablename};
            
            if (exists($sql_cmd->{alt_table_list}->{add_list}))
            {
#                greet $sql_cmd;
                
                my $alist = $sql_cmd->{alt_table_list}->{add_list};

                if (exists($alist->{type})
                    && ($alist->{type} eq 'constraint'))
                {
                    my %nargs = (
                                 tname   => $tablename,
                                 dbh_ctx => $self->{dbh_ctx}
                                 );

                    my $cons_name;
                    if (exists($alist->{cons_name}))
                    {
                        $cons_name = $alist->{cons_name};
                        $nargs{cons_name} = $cons_name;
                    }

                    if (exists($alist->{cons_type}))
                    {
                        $nargs{cons_type} = $alist->{cons_type};
                    }

                    if (exists($alist->{cons_type}))
                    { 
                       if ($alist->{cons_type} =~ m/unique|primary/i)
                       { # UNIQUE or PRIMARY KEY
                           if (exists($alist->{col_list}))
                           {
                               $nargs{cols} = $alist->{col_list};
                           }
                       }
                       elsif ($alist->{cons_type} eq 'check')
                       {

                           my $where_clause =
                               $alist->{where_clause};

                           my $filter =
                               $self->SQLWhere(tablename => $tablename,
                                               where     => $where_clause);

                           unless (defined($filter))
                           {
                               $msg = "invalid where clause";
                               %earg = (self => $self, msg => $msg,
                                        severity => 'warn');
                    
                               &$GZERR(%earg)
                                   if (defined($GZERR));

                               return 0;
                           }
#                         greet $filter;

                           $nargs{where_clause} = $filter->{where_text};
                           $nargs{where_filter} = $filter->{filter_text};

                           # XXX XXX XXX: need to get constraint name if
                           # defined by system
                       }
                       else
                       {
                           $msg = "unknown constraint\n";
                           $msg .= Dumper( %nargs);
                           %earg = (self => $self, msg => $msg,
                                    severity => 'warn');
                    
                           &$GZERR(%earg)
                               if (defined($GZERR));

                           return 0;
                       }

                       my ($stat, $new_consname, $new_iname) = 
                           $dictobj->DictTableAddConstraint(%nargs);

                       my $severity;
                       if ($stat)
                       {
                           $cons_name = $new_consname
                               unless (defined($cons_name));
                           $msg = "Added constraint $cons_name" .
                               " to table $tablename\n";
                           $severity = 'info';
                       }
                       else
                       {
                           $msg = "Failed to add constraint\n";
                           $severity = 'warn';
                       }
                       %earg = (self => $self, msg => $msg,
                                severity => $severity);
                       
                       &$GZERR(%earg)
                           if (defined($GZERR));

                       return $stat;
                   } # end if exists constraint
                }
            }
        }
    }

    return 0;
} # end SQLAlter

sub SQLUpdate
{
    my $self = shift;    
    my $dictobj = $self->{dictobj};
    my $sqltxt = $self->{current_line};

    my ($sql_cmd, $pretty, $badparse) =
        $self->{feeble}->Parseall($sqltxt);

    return undef
        if ($badparse);

#    greet $sql_cmd;

    my ($msg, %earg);
    my $severity = 'info';

    my $tablename = $sql_cmd->{tablename};

    return 0
        unless $dictobj->DictTableExists (tname => $tablename);

#    my $colcnt = scalar keys 
#        % { $dictobj->DictTableGetCols (tname => $tablename) };

    my @colary;

    for my $ii (@{$sql_cmd->{update_list}})
    {
        my ($colname, $newval) = @{$ii};

        my $colnum = $dictobj->DictTableColExists (tname => $tablename,
                                                   colname => $colname);

        if (defined($colary[$colnum]))
        {
            $msg = "duplicate update column ($colname), table ($tablename)\n";
            %earg = (self => $self, msg => $msg,
                     severity => 'warn');
                    
            &$GZERR(%earg)
                if (defined($GZERR));

            return 0;
        }

        # XXX XXX: need to handle the case where newval is a column, not
        # an identifier

        if ($newval =~ m/^NULL$/i)
        {
            # Convert unquoted NULL value to an undef
            $newval = undef;
        }
        else
        {
            # strip double and single quotes
            my @getquotes = ($newval =~ m/^\"(.*)\"$/);
            if (scalar(@getquotes))
            {
                $newval = shift @getquotes;
            }
            else
            {
                # NOTE: Special treat for single-quoted strings, 
                # e.g.: 'foo'.  Allow/require backslash as a quote
                # character, so must use '\\' to enter a single
                # backslash and '\'' (backslash quote) to embed a
                # single-quote in a single-quoted string.

                # if have leading and trailing single quote
                @getquotes = ($newval =~ m/^\'(.*)\'$/);

                if (scalar(@getquotes))
                {
#                    greet @getquotes;

                    # strip lead/trail quotes so parse_line can work
                    # its magic
                    $newval = $getquotes[0]; 

                    # use parse line to process quoted characters 

                    # XXX XXX: some weirdness using \n for delimiter
                    # -- seems to work since shouldn't have embedded
                    # newlines in these strings, so we only
                    # split the line into a single token
                    @getquotes = &parse_line('\n', 0, $newval);
#                    greet @getquotes;

                    # backslashes have been processed
                    $newval = shift @getquotes;
                    for my $tk (@getquotes)
                    {
                        $newval .= $tk
                            if (defined($tk));
                    }
                }
            }
        }

        # save newvalue for this column
        $colary[$colnum] = $newval;

    } # end for


    # build a select statement
    my $sel_cmd = {};
    $sel_cmd->{from_list} = [{val => $tablename} ];

    # get every column for now - optimize later
    $sel_cmd->{select_list} = [
                               {val => "rid", name => "rid"}, 
                               {val => "*", name => "*"}
                               ];
    $sel_cmd->{where_list} = $sql_cmd->{where_list}
       if (exists($sql_cmd->{where_list}) &&
           scalar(@{$sql_cmd->{where_list}}));
    my @sel_prep = $self->_SQLselprep($sel_cmd);
#    greet @sel_prep;

    return undef
        unless (scalar(@sel_prep));

    my @selex_state = $self->SelectExecute(@sel_prep);

    return undef 
        unless (scalar(@selex_state));

    my ($rownum, $rowcount) = (0, 0);
    my ($key, @vals, @outi);

    # select out all the rows first (consistent read)

    $msg = "";
    while (1)
    {
        ($key, $rownum, @vals) =
            $self->SelectFetch($key, $rownum, @selex_state);
        last 
            unless (defined($rownum));

#            greet $key, $rownum,  @vals;

        for my $jj (0..(scalar(@vals) - 1))
        {
            $vals[$jj] = $colary[$jj]
                if (defined($colary[$jj]));
        }
#        greet $key, $rownum,  @vals;
        
        my $newref = [@vals];
        push @outi, $newref;
    } # end while 1

    for my $ii (@outi)
    {
        my @rowarr = @{$ii};
        my $rid = shift @rowarr;
        
        unless 
            ($dictobj->RowUpdate (tname   => $tablename, 
                                  rid     => $rid,
                                  rowval  => \@rowarr,
                                  dbh_ctx => $self->{dbh_ctx}
                                  )
             )
        {
            $msg = "failed to update row $rid : \n";
            $severity = 'warn';
            last;
        }
            
        $rowcount++;

    } # end for
    my $rowthing = ((1 == $rowcount) ? "row" : "rows");
    $msg = "updated $rowcount $rowthing in table $tablename.\n";
    %earg = (self => $self, msg => $msg,
             severity => $severity);
                    
    &$GZERR(%earg)
        if (defined($GZERR));

    return $rowcount; 

} # end sqlupdate

sub SQLInsert
{
    my $self = shift;    

    my $sqltxt = $self->{current_line};

    my ($sql_cmd, $pretty, $badparse) =
        $self->{feeble}->Parseall($sqltxt);

#    greet $sql_cmd;
    return undef
        if ($badparse);

    my @outi;

    push @outi, $sql_cmd->{tablename};
    push @outi, $sql_cmd->{colnames};

    my ($key, $rownum, @vals, @selex_state);

    # if INSERT SELECT
    if (exists($sql_cmd->{selclause}))
    {
        my $colcnt = scalar(@{$sql_cmd->{colnames}});

        unless ($colcnt)
        {
            my $dictobj = $self->{dictobj};

            return undef
                unless ($dictobj->DictTableExists (tname => 
                                                   $sql_cmd->{tablename}));
            $colcnt 
                = scalar(keys(%{$dictobj->
                                    DictTableGetCols (tname => 
                                                      $sql_cmd->{tablename}
                                                      )}));
        }

#        greet $sql_cmd->{selclause};

        # internal select prepare
        my @ggg = $self->_SQLselprep($sql_cmd->{selclause});
        
        return undef
            unless (scalar(@ggg));

#        greet @ggg;
        # compare insert column list to select list
        # XXX XXX : need to fix here too
        # XXX XXX : is too few cols legal?  pad remainder with nulls?
        my $comp = ($colcnt <=> scalar(@{$ggg[2]}));
        unless (0 == $comp) # should be zero if match
        {
            my $msg = "Cannot insert: too " . (($comp == -1) ? "many": "few") .
            " columns in SELECT list\n";
            my %earg = (self => $self, msg => $msg,
                        severity => 'warn');
                    
            &$GZERR(%earg)
                if (defined($GZERR));

            return undef;
        }

        my @selex_state = $self->SelectExecute(@ggg);

        return undef 
            unless (scalar(@selex_state));

        $rownum = 0;
        
        # fetch all rows if self-modifying table -- kind of expensive...
        my $fetchall = ($sql_cmd->{tablename} eq $ggg[0]);
#        greet @ggg;

        # XXX XXX XXX: could do multiple inserts if not self-modifying table
        while (1)
        {
            ($key, $rownum, @vals) =
                $self->SelectFetch($key, $rownum, @selex_state);
            last 
                unless (defined($rownum));

#            greet $key, $rownum,  @vals;

            push @outi, @vals;
#            last
#                unless ($fetchall); XXX XXX : doesn't work right...
        }
    }
    else
    {
        if (exists($sql_cmd->{colvals}))
        {
            for my $newval (@{$sql_cmd->{colvals}})
            {

                if ($newval =~ m/^NULL$/i)
                {
                    # Convert unquoted NULL value to an undef
                    $newval = undef;
                }
                else
                {
                    # strip double and single quotes
                    my @getquotes = ($newval =~ m/^\"(.*)\"$/);
                    if (scalar(@getquotes))
                    {
                        $newval = shift @getquotes;
                    }
                    else
                    {
                        # NOTE: Special treat for single-quoted strings, 
                        # e.g.: 'foo'.  Allow/require backslash as a quote
                        # character, so must use '\\' to enter a single
                        # backslash and '\'' (backslash quote) to embed a
                        # single-quote in a single-quoted string.
                        
                        # if have leading and trailing single quote
                        @getquotes = ($newval =~ m/^\'(.*)\'$/);
                        
                        if (scalar(@getquotes))
                        {
#                            greet @getquotes;

                            # strip lead/trail quotes so parse_line can work
                            # its magic
                            $newval = $getquotes[0];

                            # use parse line to process quoted characters 
                            
                            # XXX XXX: some weirdness using \n for delimiter
                            # -- seems to work since shouldn't have embedded
                            # newlines in these strings, so we only
                            # split the line into a single token
                            @getquotes = &parse_line('\n', 0, $newval);
#                            greet @getquotes;

                            # backslashes have been processed
                            $newval = shift @getquotes;
                            for my $tk (@getquotes)
                            {
                                $newval .= $tk
                                    if (defined($tk));
                            }

                        }
                    }
                }
                push @outi, $newval;
            } # end for
        }
    }

    my $colcnt = 0;

    my $ins_stat = $self->Kgnz_Insert2(@outi);

    return $colcnt # check for insertion failure
        unless (defined($ins_stat));

    $colcnt += $ins_stat;

  L_fetchins:
    while (defined($key))
    {
        @outi = ();

        push @outi, $sql_cmd->{tablename};
        push @outi, $sql_cmd->{colnames};

        for my $ii (1..10) # do a multirow insert
        {
            ($key, $rownum, @vals) =
                $self->SelectFetch($key, $rownum, @selex_state);
            last  L_fetchins
                unless (defined($key));

            push @outi, @vals;
        }

        my $istat2 = $self->Kgnz_Insert2(@outi);

        return $colcnt
            unless (defined($istat2));

        $colcnt += $istat2;
        
    }

    return ($colcnt);

}

sub SQLDelete
{
    my $self = shift;    

    my $sqltxt = $self->{current_line};

    my ($sql_cmd, $pretty, $badparse) =
        $self->{feeble}->Parseall($sqltxt);

#    greet $sql_cmd;
    return undef
        if ($badparse);

    my @outi;

    unless (exists($sql_cmd->{tablename}))
    {
        greet $sql_cmd;
        return undef;
    }

    my $tablename = $sql_cmd->{tablename};
#    push @outi, $tables[0];

    my ($where, $where_clause);
    if (exists($sql_cmd->{where_list}))
    {
        $where = $sql_cmd->{where_list};
#        greet $where;
        for my $val (@{$where})
        {
            $where_clause .= "$val->{val} ";
        }
    }
    my $sel = "select rid from $tablename ";
    $sel .= "where " . $where_clause
        if (defined($where_clause));

#    greet $sel;

    my $ftch_aryref = $self->selectall_arrayref($sel);

#    greet @ftchary;
 
    return undef
        unless (defined($ftch_aryref));

    unless (scalar(@{$ftch_aryref}))
    {
        my $msg = "deleted 0 rows from table $tablename.\n";
        my %earg = (self => $self, msg => $msg,
                    severity => 'warn');
        
        &$GZERR(%earg)
            if (defined($GZERR));

        return 0;
    }

    my @ridlist;

    for my $ii (@{$ftch_aryref})
    {
        push @ridlist, $ii->[0];
    }
#    greet @ridlist;

    return $self->Kgnz_Delete($tablename, @ridlist);
}


sub HCountPrepare
{
    my $self = shift;
    my $dictobj = $self->{dictobj};
    my @outi;
    my $filter;

  L_sel:
    {
	last if (@_ < 1);
        
	my $tablename = shift @_ ;
        
	last unless $dictobj->DictTableExists (tname => $tablename);

        push @outi, $tablename;
        push @outi, "HCOUNT";
        push @outi, ["COUNT(*)"];
        push @outi, [
                     {name  => "COUNT(*)",
                      alias => "COUNT(*)",
                      type  => "n"}
                     ]; # no colnums
        push @outi, $filter; # filter
    }
    return @outi;
} # hcountprepare

sub HCountFetch
{
    my $self = shift;
    my ($kk, $rownum, $hashi, $sth, $seltype, $colnames, $collist) = @_; 
    my $dictobj = $self->{dictobj};
    my @outi;

  L_sel:
    {
	last if (@_ < 1);

        last if ($rownum);
        
        my $tv = tied(%{$hashi});

        push @outi, $tv->HCount();
    }

    return @outi;
} # hcountfetch

sub HCountPrint
{
    my $self = shift;
    my ($hashi, $sth, $seltype, $colnames, $collist) = @_; 
    my $rownum = 0;
    my $dictobj = $self->{dictobj};
    my $stat;

  L_sel:
    {
	last if (@_ < 1);

        print "COUNT(*)\n";
        print "--------\n";

        my $tv = tied(%{$hashi});

        print $tv->HCount(), "\n\n";

        $rownum++;

        my $msg = ($rownum ? $rownum : "no") ;
        $msg .= ((1 == $rownum) ? " row " : " rows ") .
            "selected.\n";
        my %earg = (self => $self, msg => $msg,
                    severity => 'info');
        
        &$GZERR(%earg)
            if (defined($GZERR));
        
        $stat = $rownum;            
    }

    return $stat;
} # hcount

sub ECountPrepare
{
    my $self = shift;
    my $dictobj = $self->{dictobj};
    my @outi;
    my $filter;

  L_sel:
    {
	last if (@_ < 1);
        
	my $tablename = shift @_ ;
        
	last unless $dictobj->DictTableExists (tname => $tablename);

        push @outi, $tablename;
        push @outi, "ECOUNT";
        my @colaliaslist = ("ESTIMATE", "CURRENT", "STDDEV", "PCT_COMPLETE");
        push @outi, \@colaliaslist;
        my @collist;
        for my $val (@colaliaslist)
        {
            push @collist , { # no colnums
                name  => $val,
                alias => $val,
                type  => "n" };
                
        }
        push @outi, \@collist;
        push @outi, $filter; # filter

    }
    return @outi;
} # ecountprepare

sub ECountFetch
{
    my $self = shift;
    my ($kk, $rownum, $hashi, $sth, $seltype, $colnames, $collist) = @_; 
    my $dictobj = $self->{dictobj};
    my @outi;
    my @ggg;

  L_sel:
    {
	last if (@_ < 1);

        my $tv = tied(%{$hashi});

        if ($rownum)
        {
            push @ggg, @{$kk};
        }
        else
        {
            @ggg = $tv->FirstCount();
        }

        while (scalar(@ggg) > 4)
        {
            @ggg = $tv->NextCount(@ggg);

            last
                unless (scalar(@ggg) > 4);

            my @g2 = @ggg;
               $kk = shift @g2;
            my $est    = shift @g2;
            my $sum    = shift @g2;
            my $sumsq  = 0;
            $sumsq  = shift @g2;
            my $ccnt   = shift @g2;
            my $tot    = shift @g2;
            my $pct    = ($ccnt/$tot) *100;

            my $var = 0;
            $var = ($sumsq - (($sum**2)/$ccnt))/($ccnt - 1)
                unless ($ccnt < 2); # var = 0 when numelts = 1

#        my $stddev = sqrt($sumsq);
            my $stddev = sqrt($var);

            # confidence interval : 1-alpha ~= 2 for 90% conf, 
            # 60+ samples, student-t, GAUSSIAN DATA ONLY
            #
            # mean +/-  2*stddev/sqrt(samplesize)

            my $alpha = 100; # 2

            my $conf = $alpha*$stddev/sqrt($ccnt);

            push @outi, $est,$sum,$stddev,$pct;

            last 
#                unless (defined($kk));

        } # end while
    }

    if (scalar(@outi))
    {    
#        unshift @outi, $rownum; # XXX : rownum set by selectfetch
        unshift @outi, \@ggg;
    }


    return @outi;
} # end ecountfetch

sub ECountPrint
{
    my $self = shift;
    my ($hashi, $sth, $seltype, $colnames, $colnums) = @_; 
    my $rownum = 0;
    my $dictobj = $self->{dictobj};
    my $stat;

  L_sel:
    {
	last if (@_ < 1);
        
        print "ESTIMATE\tCURRENT\tSTDDEV\tPCT_COMPLETE\n";
        print "--------\t-------\t------\t------------\n";

        my $tv = tied(%{$hashi});

        my @ggg = $tv->FirstCount();

        while (scalar(@ggg) > 4)
        {
            @ggg = $tv->NextCount(@ggg);

            my @g2 = @ggg;
            my $kk = shift @g2;
            my $est    = shift @g2;
            my $sum    = shift @g2;
            my $sumsq  = 0;
            $sumsq  = shift @g2;
            my $ccnt   = shift @g2;
            my $tot    = shift @g2;
            my $pct    = ($ccnt/$tot) *100;

            my $var = 0;
            $var = ($sumsq - (($sum**2)/$ccnt))/($ccnt - 1)
                unless ($ccnt < 2); # var = 0 when numelts = 1

#        my $stddev = sqrt($sumsq);
            my $stddev = sqrt($var);

            # confidence interval : 1-alpha ~= 2 for 90% conf, 
            # 60+ samples, student-t, GAUSSIAN DATA ONLY
            #
            # mean +/-  2*stddev/sqrt(samplesize)

            my $alpha = 100; # 2

            my $conf = $alpha*$stddev/sqrt($ccnt);

            printf "%.2f\t%d\t%.2f\t%.2f\n",
            $est,$sum,$stddev,$pct;

            $rownum++;

            last 
                unless (defined($kk));

        } # end while
        print "\n";

        my $msg = ($rownum ? $rownum : "no") ;
        $msg .= ((1 == $rownum) ? " row " : " rows ") .
            "selected.\n";
        my %earg = (self => $self, msg => $msg,
                    severity => 'info');
        
        &$GZERR(%earg)
            if (defined($GZERR));
        
        $stat = $rownum;            
    } # end l_sel

    return $stat;
} # ecountprint

sub Kgnz_Select
{
    my $self = shift;
    my @ggg = $self->SelectPrepare(basic => \@_);

    return undef
        unless (scalar(@ggg));

    my @hhh = $self->SelectExecute(@ggg);

    return undef
        unless (scalar(@hhh));

    return $self->SelectPrint(@hhh);
}

sub SelectPrepare
{
    my $self = shift;
    my $dictobj = $self->{dictobj};
    my %args = (
		@_);

    my $rxrid     = '(^rid$)';
    my $rxrownum  = '(^rownum$)';
    my $rxcols    = '(^rid$)|(^rownum$)';
    my $rxhcount  = '(^count$)';
    my $rxecount  = '(^ecount$)';
    my @outi;

    my ($tablename, $colpairs, $filter);
    
    if (defined($args{basic}))
    {
        $tablename = shift @{$args{basic}};

        $colpairs = [];
        for my $val (@{$args{basic}})
        {
            push @{$colpairs}, [$val, $val]; 
        }
    }
    else
    {
        # XXX XXX: should check these!
        unless (defined($args{tablename}))
        {
            whisper "no tablename!";
            my $msg = "no tablename";
            my %earg = (self => $self, msg => $msg,
                        severity => 'warn');
                    
            &$GZERR(%earg)
                if (defined($GZERR));

            return @outi;
        }
        $tablename = $args{tablename};
        $colpairs  = $args{colpairs};

        if (defined($args{where}))
        {
            return @outi # make sure have a table
                unless $dictobj->DictTableExists (tname => $tablename);

            $filter =
                $self->SQLWhere(tablename => $tablename,
                                where => $args{where});

            unless (defined($filter))
            {
                whisper "invalid where clause";

                my $msg = "invalid where clause";
                my %earg = (self => $self, msg => $msg,
                            severity => 'warn');
                    
                &$GZERR(%earg)
                    if (defined($GZERR));

                return @outi;
            }
        }

    }

  L_sel:
    {
	last if (@_ < 1);
        
	last unless $dictobj->DictTableExists (tname => $tablename);

        last unless (scalar(@{$colpairs}));
        
        my (@colaliaslist, @collist);
        
        my $pindx = 0;
        
      L_PPL:
        foreach my $pair (@{$colpairs})
        {
            $pindx++;

            my ($colname, $colalias) =  @{$pair};

            return $self->HCountPrepare($tablename)
                if ($colname =~ m/$rxhcount/i );

            return $self->ECountPrepare($tablename)
                if ($colname =~ m/$rxecount/i );

            if ($colname =~ m/$rxcols/i ) 
            {
                push @colaliaslist, $colalias ;
                push @collist, {colnum => lc($colname),
                                name   => lc($colname),
                                alias  => $colalias,
                                type   => # c for rid, n for rownum
                                    (($colname =~ m/$rxrid/i) ? "c" : "n")
                                };

                next L_PPL;
            }
            
            if ($colname eq '*' )
            {
                my %allcols 
                    = % { $dictobj->DictTableGetCols (tname => $tablename) };

                # $$$ $$$ need Tie::IxHash to avoid this nonsense

                # build an array of colname, colidx, coltype 
                # ordered by colidx
                while (my ($kk, $vv) = each (%allcols))
                {
                    my @rarr = @{ $vv };

                                          # colname, colidx, coltype
                    $outi[ $rarr[0]-1 ] = [$kk, @rarr] ; 
                }

                my $ccount = 1;

                foreach my $vv (@outi)
                { 
                    my $val     = $vv->[0];
                    my $coltype = $vv->[2];

                    push @colaliaslist, $val ; # no alias
                    push @collist, {colnum => $ccount,
                                    name   => $val,
                                    alias  => $val,
                                    type   => $coltype
                                };

                    $ccount++;
                }
                next L_PPL;
            }
     
            my ($colnum, $coltype)
                = $dictobj->DictTableColExists (tname => $tablename,
                                                colname => $colname);
            if ($colnum)
            {
                push @colaliaslist, $colalias ;
                push @collist, {colnum => $colnum,
                                name   => $colname,
                                alias  => $colalias,
                                type   => $coltype
                                };

                next L_PPL;
            }

            last L_sel; # failed
        }

        @outi = (); # clear colnames

        push @outi, $tablename;
        push @outi, "SELECT";
        # Note: save the column alias list for GStatement::execute
        push @outi, \@colaliaslist;
        push @outi, \@collist;
        push @outi, $filter;

    }

    return @outi;
} # end SelectPrepare

sub SelectExecute
{
    my $self = shift @_;
    my $tablename = shift @_;
    my $filter = pop @_;
    my $dictobj = $self->{dictobj};
    my @outi;

#    greet $filter;

    return @outi
        unless (defined($tablename));

    my $hashi = $dictobj->DictTableGetTable (tname   => $tablename,
                                             dbh_ctx => $self->{dbh_ctx}) ;

    return @outi
        unless (defined($hashi));

    my $sth;

    # XXX XXX: ok to sqlexecute even for hcount, ecount
    {
        my $tv = tied(%{$hashi});
        my %prep;
        $prep{filter} = $filter    # fix for hcount/ecount
            if (defined($filter)); # where filter is undef
        $sth = $tv->SQLPrepare(%prep);
        return @outi
            unless ($sth->SQLExecute());
    }

    push @outi, $hashi, $sth;
    push @outi, @_;
    return @outi;
}

sub SelectFetch
{
    my $self = shift;
    my ($kk, $rownum, $hashi, $sth, $seltype, $colnames, $collist) = @_;
    my $dictobj = $self->{dictobj};
    my $rxrid     = '(^rid$)';
    my $rxrownum  = '(^rownum$)';
    my $rxcols    = '(^rid$)|(^rownum$)';
    my $rxhcount  = '(^count$)';
    my $rxecount  = '(^ecount$)';
    my @outi;

    if ($seltype =~ m/^HCOUNT$/)
    {
        @outi = $self->HCountFetch(@_);
#        greet @outi;
    }
    elsif ($seltype =~ m/^ECOUNT$/)
    {
        ($kk, @outi) = $self->ECountFetch(@_);
    }
    else
    {
        my $tv = tied(%{$hashi});

        while (1)
        {
            my $vv;

            ($kk, $vv) = $sth->SQLFetch($kk);

            last
                unless (defined($kk));

            next # XXX XXX: skip bad rows
                unless (defined($vv));
            my @rarr = @{ $vv };

	    foreach my $coldef (@{$collist})
	    {
                my $colnum = $coldef->{colnum};

                if ($colnum =~ m/$rxrid/i )
                {
#                print $kk ; 
                    push @outi, $kk;
                }
                elsif ($colnum =~ m/$rxrownum/i )
                {
#                print $rownum ;
                    # NOTE: rownum only incremented after
                    # column list processed correctly
                    push @outi, ($rownum + 1);
                }
                else
                {
                    my $rval = $rarr[$colnum-1];
#                    $rval = '<undef>' # NOTE: deal with undefs
#                        unless (defined($rval));
                    
#                print $rval ;
                    push @outi, $rval;
                }
            }
            last;
        } # end while
    }

    if (scalar(@outi))
    {
        $rownum++;
        unshift @outi, $rownum;
        unshift @outi, $kk;
    }

#    greet @outi;

    return @outi;
	
} # end selectfetch

sub SelectPrint
{
    my $self = shift;
    my ($hashi, $sth, $seltype, $colnames, $collist) = @_;
    my $dictobj = $self->{dictobj};
    my $rxrid     = '(^rid$)';
    my $rxrownum  = '(^rownum$)';
    my $rxcols    = '(^rid$)|(^rownum$)';
    my $rxhcount  = '(^count$)';
    my $rxecount  = '(^ecount$)';
    my $stat;

    if ($seltype =~ m/^HCOUNT$/)
    {
        return $self->HCountPrint(@_);
    }
    elsif ($seltype =~ m/^ECOUNT$/)
    {
        return $self->ECountPrint(@_);
    }

    {
        # print column name headers
        foreach my $coldef (@{$collist})
        {
            print $coldef->{alias}, "\t";
        }
        print "\n";
        foreach  my $coldef2 (@{$collist})
        {
            print '_' x length($coldef2->{alias}), "\t";
        }
        print "\n";
        print "\n";

        # print the columns

	# use "each" to get pairs versus "keys", which prefetches
	# entire hash
        my $rownum = 0;

        while (1)
        {
            my ($kk, $vv) = $sth->SQLFetch();

            last
                unless (defined($kk));

            next # XXX XXX: skip bad rows
                unless (defined($vv));
            my @rarr = @{ $vv };

            $rownum++;

	    foreach my $coldef (@{$collist})
	    {
                my $colnum = $coldef->{colnum};
                if ($colnum =~ m/$rxrid/i )
                {
                    print $kk ; 
                }
                elsif ($colnum =~ m/$rxrownum/i )
                {
                    print $rownum ;
                }
                else
                {
                    my $rval = $rarr[$colnum-1];
                    $rval = '<undef>' # NOTE: deal with undefs
                        unless (defined($rval));
                    
                    print $rval ;
                }
                print "\t";
	    }

            print "\n";
	}
        print "\n";
        my $msg = ($rownum ? $rownum : "no") ;
        $msg .= ((1 == $rownum) ? " row " : " rows ") .
            "selected.\n";
        my %earg = (self => $self, msg => $msg,
                    severity => 'info');
        
        &$GZERR(%earg)
            if (defined($GZERR));
        
        $stat = $rownum;            
    }

    return $stat;
	
} # end selectprint


my %parsedispatch =
qw(
   help   Kgnz_Help
   quit   Kgnz_Quit 
   reload Kgnz_Reload
   dump   Kgnz_Dump
   spool  Kgnz_Spool

   h       Kgnz_History
   history Kgnz_History

   rem    Kgnz_Rem

   commit    Kgnz_Commit
   rollback Kgnz_Rollback

   desc     Kgnz_Describe
   describe Kgnz_Describe

   ci     Kgnz_CIdx

   ct     Kgnz_CT
   dt     Kgnz_Drop
   drop   Kgnz_Drop

   alter  SQLAlter
   create SQLCreate

   i      Kgnz_Insert
   insert SQLInsert

   update SQLUpdate
   delete SQLDelete
   u      Kgnz_Update
   d      Kgnz_Delete

   s      Kgnz_Select
   select SQLSelect

   addfile  Kgnz_AddFile
   af       Kgnz_AddFile

   end    Kgnz_BigStatement

   show     Kgnz_Show

   startup  Kgnz_Startup
   shutdown Kgnz_Shutdown 
   password Kgnz_Password

   );

my %opdispatch =
qw(
   create Kgnz_Create
   );


sub histpush
{
    my $self = shift;
    my ($hcnt, $val) = @_;
    my $histlist = $self->{histlist};
    push @{$histlist}, [$hcnt, $val];

    while (scalar(@{$histlist}) > $self->{maxhist})
    {
        shift @{$histlist} ;
    }

}

sub histfetch
{
#    greet @_;
    my $self = shift;
    my ($getcnt) = shift @_;
    my $histlist = $self->{histlist};
    my $aval = $histlist->[0];
    my ($hcnt, $val) = @{$aval};

    {
        last if ($getcnt < $hcnt);
        last if ($getcnt > ($hcnt + scalar(@{$histlist})));

        my $hidx = $getcnt - $hcnt;

        return $histlist->[$hidx];
    }

    my $msg = "!" . $getcnt . ": event not found\n";
    my %earg = (self => $self, msg => $msg,
                severity => 'warn');
        
    &$GZERR(%earg)
        if (defined($GZERR));

    return undef;
}

sub Kgnz_History
{
    my $self = shift;
    my $harg = shift @_;
    my $histlist = $self->{histlist};

    my ($msg, %earg);

    if (defined($harg) && ($harg =~ m/clear/i))
    {
        $msg = "Cleared history...\n";
        %earg = (self => $self, msg => $msg,
                 severity => 'info');
        
        &$GZERR(%earg)
            if (defined($GZERR));

        $self->{histlist} = [];
        return 1;
    }

    $msg = "\n";
    foreach my $aval (@{$histlist})
    {
        my ($hcnt, $val) = @{$aval};
        
        $msg .= $hcnt . " " . $val . "\n";
    }
        %earg = (self => $self, msg => $msg,
                 severity => 'info');
        
        &$GZERR(%earg)
            if (defined($GZERR));

}

sub Kgnz_Help
{
    my $self = shift;
#    print Dumper(%parsedispatch) ;
    my $bigHelp;
    $bigHelp = <<EOF_HELP;

@ : execute a command file.  Syntax - @<filename> 

!<number>, !! : re-execute command history

addfile, af : add a file to a tablespace.  Type "addfile help" for more 
    details.

alter : SQL alter

ci : create index.  Syntax - ci <index name> <table name> <column name>.

commit : flush changes to disk

create : SQL Create

ct : create table.  Syntax - 
    ct <tablename> <column name>=<column type> [<column name>=<column type>...]
    Supported types are "c" for character data and "n" for numeric.

d : delete from a table.  Syntax - d <table-name> <rid> [<rid>...]

delete : SQL Delete

describe, desc :  describe a table

drop :  SQL Drop

dt : drop table.  Syntax - dt <table-name>

dump :  dump internal state.  Type "dump help" for more details.

help :  This page.

history, h : command history.  Use shell-style "!<command-number>" to repeat.

i : insert into a table. 
    Syntax - i <tablename> <column-data> [<column-data>...]

insert : SQL Insert

password : password authentication [unused]

quit : quit the line-mode app

reload : Reload all genezzo modules

rem : Remark [comment]

rollback : discard uncommitted changes

s : select from a table.  
    Syntax - s <table-name> *
             s <table-name> <column-name> [<column-name>...]
    Legal "pseudo-columns" are "rid", "rownum".

select : SQL Select
   
show :  License, warranty, and version information.  Type "show help"
        for more information.

shutdown : shutdown an instance.  Provides read-only access to "pref1"
           table.
 
spool : write output to a file.  Syntax - spool <filename>

startup : Loads dictionary, provides read/write access to tables.

u : update a table.  
    Syntax - u <table-name> <rid> <column-value> [<column-value>...]

update : SQL Update

EOF_HELP

    my $msg = $bigHelp;
    my %earg = (self => $self, msg => $msg,
                severity => 'info');
        
    &$GZERR(%earg)
        if (defined($GZERR));

    return 1;
}

sub Kgnz_Prepare
{
    my ($self, $currline) = @_;

    return undef
        unless (defined($currline));

    $self->{current_line} = $currline;
    my @pwords = shellwords($currline);

    return undef
        unless (@pwords);

    my ($msg, %earg);
    my $severity = 'info';
    
    my $operation;

    while (1)
    {
        $operation = shift @pwords ;
        last # pop off empties to find keyword
            if ($operation =~ /\S/);

    }

  L_beginend:
    {
        if ($self->{endwait})
        {
            if (uc($operation) eq 'END')
            {
                $self->{endwait} = 0;
#                whisper $self->{bigstatement}, "\n";
                
                $pwords[0] = $self->{bigstatement};
                last L_beginend;   
            }
            
            $self->{bigstatement} .= $operation;
            $self->{bigstatement} .= ' ';
            
            while (my $thing = shift @pwords) 
            {
                $self->{bigstatement} .= $thing;
                $self->{bigstatement} .= ' ';
            }
            return undef;
            
        }
        if (uc($operation) eq 'BEGIN')
        {
            $self->{bigstatement} = ();
            $self->{endwait} = 1;
            return undef;
        }
    } # end L_beginend;

    # @file to execute commands
    unless (@pwords)
    {
        if ($operation =~ m/^\!/)
        {
            my $hhnum = ();

            if ($operation eq "!!")
            {
                $hhnum = $self->{histcounter} - 1;
            }
            else
            {

                my @hnum = ($operation =~ m/^\!(\d.*)/);

#        whisper @hnum;
                $hhnum = $hnum[0];
            }

            if (defined($hhnum))
            {
                pop @{$self->{histlist}};
                my $aval = $self->histfetch($hhnum);
                return undef
                    unless (defined($aval));
                
                my ($hcnt, $val) = @{$aval};
                $self->histpush ($self->{histcounter}, $val);
                print "$val\n";
                return $self->Kgnz_Prepare($val);
            }
        }

	my @pfiles = split(/(@)/, $operation) ;

        $msg = "";
	{
	    last if (@pfiles < 2 );

	  L_inifile:
	    foreach my $inifile (@pfiles)
	    {
		next if ($inifile eq '');
		next if ($inifile eq '@');

		unless (-e $inifile)
		{
		    $msg .= "file $inifile does not exist \n";
                    $severity = 'warn';
		    last L_inifile;
		}

                my $fh; # lexical scope filehandle for nesting includes
		unless (open ($fh, "< $inifile" ) )
                {
		    $msg .="Could not open $inifile for reading : $! \n";
                    $severity = 'warn';
		    last  L_inifile;                    
                }

		while (<$fh>) {
		    print "$inifile> $_ \n";
                    my $in_line = $_;
                    $in_line =~ s/;(\s*)$//; # XXX: remove the semicolon
                    $self->Parseall ($in_line);

		} # end big while
		close ($fh);
	    } # end foreach

            if ($severity !~ m/info/i)
            {
                %earg = (self => $self, msg => $msg,
                         severity => $severity);
        
                &$GZERR(%earg)
                    if (defined($GZERR));
            }

	    return undef;
	}
    }

    unless (exists($parsedispatch{lc($operation)}))
    {
	$msg = "could not parse: " .
            Dumper ($operation) . Dumper (@pwords) . "\n" ;

        %earg = (self => $self, msg => $msg,
                 severity => 'warn');
        
        &$GZERR(%earg)
            if (defined($GZERR));

	return undef;
    }

    my $dispatch = $parsedispatch{lc($operation)};

    unshift @pwords, $dispatch;

    return @pwords;

} # end Kgnz_Prepare

sub Kgnz_Execute
{
    my ($self, $dispatch, @pwords) = @_;

    return undef # no dispatch function if parse failed...
        unless (defined($dispatch));

    no strict 'refs' ;
    my $stat = &$dispatch ($self, @pwords) ;
    return $stat;
}

sub Parseall 
{
    my ($self, $currline) = @_;
    $self->_clearerror();
    my @param = $self->Kgnz_Prepare($currline);
    return undef
        unless (scalar(@param));

    return $self->Kgnz_Execute(@param);
}

sub do # DBI
{
    my $self = shift;
    return $self->Parseall(@_);
}

sub prepare # DBI
{
    my ($self, $currline) = @_;
    $self->_clearerror();
    my @param = $self->Kgnz_Prepare($currline);
    return undef
        unless (scalar(@param));

    my $sth = Genezzo::GStatement->new(gnz_h     => $self, 
                                       dbh_ctx   => $self->{dbh_ctx},
                                       GZERR     => $self->{GZERR},
                                       statement => \@param);
    return $sth;
}

sub selectrow_array # DBI
{
    my $self = shift;

    my $sth = $self->prepare(@_);
    return undef
        unless (defined($sth));

    return $sth->_selectrow_internal("ARRAY");
}
sub selectrow_arrayref # DBI
{
    my $self = shift;

    my $sth = $self->prepare(@_);
    return undef
        unless (defined($sth));

    return $sth->_selectrow_internal("ARRAYREF");
}
sub selectall_arrayref # DBI
{
    my $self = shift;

    my $sth = $self->prepare(@_);
    return undef
        unless (defined($sth));

    return $sth->_selectrow_internal("ALL_ARRAYREF");
}
sub selectrow_hashref # DBI
{
    my $self = shift;

    my $sth = $self->prepare(@_);
    return undef
        unless (defined($sth));

    return $sth->_selectrow_internal("HASHREF");
}

sub Kgnz_BigStatement
{
    my $self = shift;
    {
	last if (@_ < 1);

	my $bigstatement = shift @_;

        my %args = ();
        {
            no strict;

            eval "$bigstatement";

            use strict;
        }

        unless (   (exists ($args{op1})) 
                && (exists ($opdispatch{lc($args{op1})})))
        {
            my $msg = "Could not find valid operation in: \n" . 
                "$bigstatement \n";
            my %earg = (self => $self, msg => $msg,
                        severity => 'warn');
            
            &$GZERR(%earg)
                if (defined($GZERR));

            return 0;
        }
        my $dispatch = $opdispatch{lc($args{op1})};

        no strict 'refs' ;
        return &$dispatch (%args) ;
    }
    return 0;
}

# check preferences for automatic mount
sub automountcheck
{
    my $self = shift;
    my $dictobj = $self->{dictobj};

    my $hashi = $dictobj->DictTableGetTable (tname   => '_pref1',
                                             dbh_ctx => $self->{dbh_ctx}
                                             ) ;

    while ( my ($kk, $vv) = each ( %{$hashi}))
    { 
        my @rarr = @{ $vv };

        if ($rarr[0] =~ m/automount/)
        {
            my $amval = $rarr[1] ;
            my $msg = "automount = $amval\n";
            if ($rarr[1] =~ m/TRUE/)
            {
                $msg .= "automounting...\n";
            }
            my %earg = (self => $self, msg => $msg,
                        severity => 'info');
        
            &$GZERR(%earg)
                if (defined($GZERR));

            if ($rarr[1] =~ m/TRUE/)
            {
                return $self->Kgnz_Startup();
            }
            last;
        }
    }
    return 0;
}

sub Interactive
{
    my $self = shift;

    unless (defined($self->{dictobj}))
    {
        return undef; # no dictionary
    }

    $self->automountcheck();

    $self->PrintVersionString();

    my $term = new Term::ReadLine 'gendba';

#    greet $term->Features ;

    my $prompt = "\ngendba $self->{histcounter}> ";

    my $in_line;

    while ( defined ($in_line = $term->readline($prompt)))
    {
        $in_line =~ s/;(\s*)$//; # XXX: remove the semicolon
        next unless ($in_line =~ m/\S/);
        $term->addhistory($in_line);
        $self->histpush($self->{histcounter}, $in_line);
        $self->Parseall ($in_line);
        ($self->{histcounter}) += 1;
        $prompt = "\ngendba $self->{histcounter}> ";
    } # end big while

    return 1; 
}

sub PrintLicense
{
    my $self = shift;
    my $bigGPL;
    ($bigGPL = <<EOF_GPL) =~ s/^\#//gm;
#
#		    GNU GENERAL PUBLIC LICENSE
#		       Version 2, June 1991
#
# Copyright (C) 1989, 1991 Free Software Foundation, Inc.
#     59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
# Everyone is permitted to copy and distribute verbatim copies
# of this license document, but changing it is not allowed.
#
#			    Preamble
#
#  The licenses for most software are designed to take away your
#freedom to share and change it.  By contrast, the GNU General Public
#License is intended to guarantee your freedom to share and change free
#software--to make sure the software is free for all its users.  This
#General Public License applies to most of the Free Software
#Foundation's software and to any other program whose authors commit to
#using it.  (Some other Free Software Foundation software is covered by
#the GNU Library General Public License instead.)  You can apply it to
#your programs, too.
#
#  When we speak of free software, we are referring to freedom, not
#price.  Our General Public Licenses are designed to make sure that you
#have the freedom to distribute copies of free software (and charge for
#this service if you wish), that you receive source code or can get it
#if you want it, that you can change the software or use pieces of it
#in new free programs; and that you know you can do these things.
#
#  To protect your rights, we need to make restrictions that forbid
#anyone to deny you these rights or to ask you to surrender the rights.
#These restrictions translate to certain responsibilities for you if you
#distribute copies of the software, or if you modify it.
#
#  For example, if you distribute copies of such a program, whether
#gratis or for a fee, you must give the recipients all the rights that
#you have.  You must make sure that they, too, receive or can get the
#source code.  And you must show them these terms so they know their
#rights.
#
#  We protect your rights with two steps: (1) copyright the software, and
#(2) offer you this license which gives you legal permission to copy,
#distribute and/or modify the software.
#
#  Also, for each author's protection and ours, we want to make certain
#that everyone understands that there is no warranty for this free
#software.  If the software is modified by someone else and passed on, we
#want its recipients to know that what they have is not the original, so
#that any problems introduced by others will not reflect on the original
#authors' reputations.
#
#  Finally, any free program is threatened constantly by software
#patents.  We wish to avoid the danger that redistributors of a free
#program will individually obtain patent licenses, in effect making the
#program proprietary.  To prevent this, we have made it clear that any
#patent must be licensed for everyone's free use or not licensed at all.
#
#  The precise terms and conditions for copying, distribution and
#modification follow.
#
#		    GNU GENERAL PUBLIC LICENSE
#   TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION
#
#  0. This License applies to any program or other work which contains
#a notice placed by the copyright holder saying it may be distributed
#under the terms of this General Public License.  The "Program", below,
#refers to any such program or work, and a "work based on the Program"
#means either the Program or any derivative work under copyright law:
#that is to say, a work containing the Program or a portion of it,
#either verbatim or with modifications and/or translated into another
#language.  (Hereinafter, translation is included without limitation in
#the term "modification".)  Each licensee is addressed as "you".
#
#Activities other than copying, distribution and modification are not
#covered by this License; they are outside its scope.  The act of
#running the Program is not restricted, and the output from the Program
#is covered only if its contents constitute a work based on the
#Program (independent of having been made by running the Program).
#Whether that is true depends on what the Program does.
#
#  1. You may copy and distribute verbatim copies of the Program's
#source code as you receive it, in any medium, provided that you
#conspicuously and appropriately publish on each copy an appropriate
#copyright notice and disclaimer of warranty; keep intact all the
#notices that refer to this License and to the absence of any warranty;
#and give any other recipients of the Program a copy of this License
#along with the Program.
#
#You may charge a fee for the physical act of transferring a copy, and
#you may at your option offer warranty protection in exchange for a fee.
#
#  2. You may modify your copy or copies of the Program or any portion
#of it, thus forming a work based on the Program, and copy and
#distribute such modifications or work under the terms of Section 1
#above, provided that you also meet all of these conditions:
#
#    a) You must cause the modified files to carry prominent notices
#    stating that you changed the files and the date of any change.
#
#    b) You must cause any work that you distribute or publish, that in
#    whole or in part contains or is derived from the Program or any
#    part thereof, to be licensed as a whole at no charge to all third
#    parties under the terms of this License.
#
#    c) If the modified program normally reads commands interactively
#    when run, you must cause it, when started running for such
#    interactive use in the most ordinary way, to print or display an
#    announcement including an appropriate copyright notice and a
#    notice that there is no warranty (or else, saying that you provide
#    a warranty) and that users may redistribute the program under
#    these conditions, and telling the user how to view a copy of this
#    License.  (Exception: if the Program itself is interactive but
#    does not normally print such an announcement, your work based on
#    the Program is not required to print an announcement.)
#
#These requirements apply to the modified work as a whole.  If
#identifiable sections of that work are not derived from the Program,
#and can be reasonably considered independent and separate works in
#themselves, then this License, and its terms, do not apply to those
#sections when you distribute them as separate works.  But when you
#distribute the same sections as part of a whole which is a work based
#on the Program, the distribution of the whole must be on the terms of
#this License, whose permissions for other licensees extend to the
#entire whole, and thus to each and every part regardless of who wrote it.
#
#Thus, it is not the intent of this section to claim rights or contest
#your rights to work written entirely by you; rather, the intent is to
#exercise the right to control the distribution of derivative or
#collective works based on the Program.
#
#In addition, mere aggregation of another work not based on the Program
#with the Program (or with a work based on the Program) on a volume of
#a storage or distribution medium does not bring the other work under
#the scope of this License.
#
#  3. You may copy and distribute the Program (or a work based on it,
#under Section 2) in object code or executable form under the terms of
#Sections 1 and 2 above provided that you also do one of the following:
#
#    a) Accompany it with the complete corresponding machine-readable
#    source code, which must be distributed under the terms of Sections
#    1 and 2 above on a medium customarily used for software interchange; or,
#
#    b) Accompany it with a written offer, valid for at least three
#    years, to give any third party, for a charge no more than your
#    cost of physically performing source distribution, a complete
#    machine-readable copy of the corresponding source code, to be
#    distributed under the terms of Sections 1 and 2 above on a medium
#    customarily used for software interchange; or,
#
#    c) Accompany it with the information you received as to the offer
#    to distribute corresponding source code.  (This alternative is
#    allowed only for noncommercial distribution and only if you
#    received the program in object code or executable form with such
#    an offer, in accord with Subsection b above.)
#
#The source code for a work means the preferred form of the work for
#making modifications to it.  For an executable work, complete source
#code means all the source code for all modules it contains, plus any
#associated interface definition files, plus the scripts used to
#control compilation and installation of the executable.  However, as a
#special exception, the source code distributed need not include
#anything that is normally distributed (in either source or binary
#form) with the major components (compiler, kernel, and so on) of the
#operating system on which the executable runs, unless that component
#itself accompanies the executable.
#
#If distribution of executable or object code is made by offering
#access to copy from a designated place, then offering equivalent
#access to copy the source code from the same place counts as
#distribution of the source code, even though third parties are not
#compelled to copy the source along with the object code.
#
#  4. You may not copy, modify, sublicense, or distribute the Program
#except as expressly provided under this License.  Any attempt
#otherwise to copy, modify, sublicense or distribute the Program is
#void, and will automatically terminate your rights under this License.
#However, parties who have received copies, or rights, from you under
#this License will not have their licenses terminated so long as such
#parties remain in full compliance.
#
#  5. You are not required to accept this License, since you have not
#signed it.  However, nothing else grants you permission to modify or
#distribute the Program or its derivative works.  These actions are
#prohibited by law if you do not accept this License.  Therefore, by
#modifying or distributing the Program (or any work based on the
#Program), you indicate your acceptance of this License to do so, and
#all its terms and conditions for copying, distributing or modifying
#the Program or works based on it.
#
#  6. Each time you redistribute the Program (or any work based on the
#Program), the recipient automatically receives a license from the
#original licensor to copy, distribute or modify the Program subject to
#these terms and conditions.  You may not impose any further
#restrictions on the recipients' exercise of the rights granted herein.
#You are not responsible for enforcing compliance by third parties to
#this License.
#
#  7. If, as a consequence of a court judgment or allegation of patent
#infringement or for any other reason (not limited to patent issues),
#conditions are imposed on you (whether by court order, agreement or
#otherwise) that contradict the conditions of this License, they do not
#excuse you from the conditions of this License.  If you cannot
#distribute so as to satisfy simultaneously your obligations under this
#License and any other pertinent obligations, then as a consequence you
#may not distribute the Program at all.  For example, if a patent
#license would not permit royalty-free redistribution of the Program by
#all those who receive copies directly or indirectly through you, then
#the only way you could satisfy both it and this License would be to
#refrain entirely from distribution of the Program.
#
#If any portion of this section is held invalid or unenforceable under
#any particular circumstance, the balance of the section is intended to
#apply and the section as a whole is intended to apply in other
#circumstances.
#
#It is not the purpose of this section to induce you to infringe any
#patents or other property right claims or to contest validity of any
#such claims; this section has the sole purpose of protecting the
#integrity of the free software distribution system, which is
#implemented by public license practices.  Many people have made
#generous contributions to the wide range of software distributed
#through that system in reliance on consistent application of that
#system; it is up to the author/donor to decide if he or she is willing
#to distribute software through any other system and a licensee cannot
#impose that choice.
#
#This section is intended to make thoroughly clear what is believed to
#be a consequence of the rest of this License.
#
#  8. If the distribution and/or use of the Program is restricted in
#certain countries either by patents or by copyrighted interfaces, the
#original copyright holder who places the Program under this License
#may add an explicit geographical distribution limitation excluding
#those countries, so that distribution is permitted only in or among
#countries not thus excluded.  In such case, this License incorporates
#the limitation as if written in the body of this License.
#
#  9. The Free Software Foundation may publish revised and/or new versions
#of the General Public License from time to time.  Such new versions will
#be similar in spirit to the present version, but may differ in detail to
#address new problems or concerns.
#
#Each version is given a distinguishing version number.  If the Program
#specifies a version number of this License which applies to it and "any
#later version", you have the option of following the terms and conditions
#either of that version or of any later version published by the Free
#Software Foundation.  If the Program does not specify a version number of
#this License, you may choose any version ever published by the Free Software
#Foundation.
#
#  10. If you wish to incorporate parts of the Program into other free
#programs whose distribution conditions are different, write to the author
#to ask for permission.  For software which is copyrighted by the Free
#Software Foundation, write to the Free Software Foundation; we sometimes
#make exceptions for this.  Our decision will be guided by the two goals
#of preserving the free status of all derivatives of our free software and
#of promoting the sharing and reuse of software generally.
#
#			    NO WARRANTY
#
#  11. BECAUSE THE PROGRAM IS LICENSED FREE OF CHARGE, THERE IS NO WARRANTY
#FOR THE PROGRAM, TO THE EXTENT PERMITTED BY APPLICABLE LAW.  EXCEPT WHEN
#OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER PARTIES
#PROVIDE THE PROGRAM "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESSED
#OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
#MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.  THE ENTIRE RISK AS
#TO THE QUALITY AND PERFORMANCE OF THE PROGRAM IS WITH YOU.  SHOULD THE
#PROGRAM PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL NECESSARY SERVICING,
#REPAIR OR CORRECTION.
#
#  12. IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING
#WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MAY MODIFY AND/OR
#REDISTRIBUTE THE PROGRAM AS PERMITTED ABOVE, BE LIABLE TO YOU FOR DAMAGES,
#INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL OR CONSEQUENTIAL DAMAGES ARISING
#OUT OF THE USE OR INABILITY TO USE THE PROGRAM (INCLUDING BUT NOT LIMITED
#TO LOSS OF DATA OR DATA BEING RENDERED INACCURATE OR LOSSES SUSTAINED BY
#YOU OR THIRD PARTIES OR A FAILURE OF THE PROGRAM TO OPERATE WITH ANY OTHER
#PROGRAMS), EVEN IF SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE
#POSSIBILITY OF SUCH DAMAGES.
#
#		     END OF TERMS AND CONDITIONS
#
EOF_GPL

    my $msg = "\nThe Genezzo program may be redistributed under terms of\n" .
    "the GNU General Public License.\n" . $bigGPL;
    my %earg = (self => $self, msg => $msg,
                severity => 'info');
        
    &$GZERR(%earg)
        if (defined($GZERR));

} # end printlicense

package Genezzo::GStatement;
use strict;
use warnings;
use Genezzo::Util;

sub _init
{
    my $self = shift;
    my %args = (@_);

    return 0
        unless (exists($args{gnz_h}));

    $self->{gnz_h}      = $args{gnz_h};
    $self->{PrintError} = $self->{gnz_h}->{PrintError};
    $self->{RaiseError} = $self->{gnz_h}->{RaiseError};

    if (exists($args{statement}))
    {
#        greet $args{statement};
        $self->{param} = $args{statement};
        my $match1 = '(^Kgnz_Select$)';
        my $match2 = '(^SQLSelect$)';

        if (scalar(@{$self->{param}}))
        {
            if ($self->{param}->[0] =~ m/$match1/ )
            {
                shift @{$self->{param}};
                $self->{select} = [];
                push @{$self->{select}},
                    $self->{gnz_h}->SelectPrepare(basic =>
                                                  \@{$self->{param}});

                # check if prepare failed
                return 0
                    unless scalar(@{$self->{select}});
            }
            elsif ($self->{param}->[0] =~ m/$match2/ )
            {
                shift @{$self->{param}};
                $self->{select} = [];
                push @{$self->{select}},
                    $self->{gnz_h}->SQLSelectPrepare(@{$self->{param}});

                # check if prepare failed
                return 0
                    unless scalar(@{$self->{select}});
            }
        }

    }

    $self->{rownum} = -1;
    $self->{state} = "PREPARE";
    return 1;
}

sub _clearerror
{
    my $self = shift;
    $self->{errstr} = undef;
    $self->{err}    = undef;
}

sub new
{
 #   whoami;
    my $invocant = shift;
    my $class = ref($invocant) || $invocant ; 
    my $self = { };


    my %args = (@_);

    $self->{GZERR} = $args{GZERR};

    return undef
        unless (_init($self,%args));

    return bless $self, $class;

} # end new

sub execute
{
#    whoami;
    my $self = shift;
    $self->_clearerror();

    unless (exists($self->{select}))
    {    
        my $stat = $self->{gnz_h}->Kgnz_Execute(@{$self->{param}});

        # get the number of rows affected by insert/update/delete
        if ($self->{param}->[0] =~ 
            m/(?i)^(Kgnz_Insert2|SQLInsert|SQLUpdate|Kgnz_Update|Kgnz_Delete|SQLDelete)$/)
        {
#            greet $self->{param}->[0];
            $self->{rownum} = $stat;
        }

        $self->{state} = "EXECUTE"
            if (defined($stat));

        return $stat;
    }

#    greet $self->{select};

    $self->{sel_ex} = [];
    push @{$self->{sel_ex}},
        $self->{gnz_h}->SelectExecute(@{$self->{select}});

    if (scalar(@{$self->{sel_ex}}))
    {
        $self->{state} = "EXECUTE";

        $self->{rownum} = 0;

        # see DBI Statement Handle Attributes

        # XXX XXX: too fragile - make a hash
        $self->{NUM_OF_FIELDS} = 
            scalar(@{$self->{sel_ex}->[3]}); # colnames

        $self->{NAME} = 
           $self->{sel_ex}->[3]; # colnames

        return 1;
    }
    $self->{rownum} = -1;
    return undef;
}

sub rows
{
    return $_[0]->{rownum};
}

sub fetch
{
    my $self = shift;
    $self->_clearerror();

    return $self->fetchrow_arrayref();
}

sub _fetchrow_internal
{
    my ($self, $fetchtype) = @_;
    $self->_clearerror();

    if (!defined($fetchtype) ||
        ($fetchtype =~ m/^ARRAY$/))
    {
        return $self->fetchrow_array();
    }

    return $self->fetchall_arrayref()
        if ($fetchtype =~ m/^ALL_ARRAYREF$/);

    my @val = $self->fetchrow_array();

    return undef
        unless (scalar(@val));

    return \@val # ARRAYREF
        if ($fetchtype =~ m/^ARRAYREF$/);

    # else hashref
    return undef
        unless ($fetchtype =~ m/^HASHREF$/);

    # XXX XXX: fix here too
    my $colnames = $self->{sel_ex}->[3];

    my $outi = {};

    for my $i (0..scalar(@{$colnames}))
    {
        my $v1 = $colnames->[$i];
        $outi->{$v1} = shift @val;
        last
            unless (scalar(@val));
    }

    return $outi;
} # end _fetchrow_internal

sub fetchall_arrayref
{
    my $self = shift;

    my @outi;
    while (1)
    {
        my $ary_ref = $self->fetchrow_arrayref();

        last 
            unless (defined($ary_ref));
        push @outi, $ary_ref;
    }

    return \@outi;
}

sub fetchrow_arrayref
{
    my $self = shift;
    return $self->_fetchrow_internal("ARRAYREF");
}
sub fetchrow_hashref
{
#    whoami;
    my $self = shift;
    return $self->_fetchrow_internal("HASHREF");
}

sub _selectrow_internal
{
#    whoami;
    my $self = shift;
    
    return undef
        unless (defined($self->execute()));
    return $self->_fetchrow_internal(@_);
}

sub fetchrow_array
{
    my $self = shift;
    $self->_clearerror();

    return undef
        unless (
                ($self->{state} eq "EXECUTE")
                && exists($self->{sel_ex}));

    # XXX : should we change state to "fetch"?  Should we be able to
    # re-execute in the middle of a fetch?

#    greet $self->{prevkey}, $self->{rownum};

    my ($key, $rownum, @vals) = 
        $self->{gnz_h}->SelectFetch(
                                    $self->{prevkey},
                                    $self->{rownum},
                                    @{$self->{sel_ex}});
#    greet $k2, $rownum;
    $self->{prevkey} = $key;
    $self->{rownum} = $rownum
        if (defined($rownum));

    # XXX : should we change state to EOF (end of fetch) when key is null?

#    greet @vals;
    return @vals;
}


# Autoload methods go after =cut, and are processed by the autosplit program.

1;
__END__
# Below is stub documentation for your module. You better edit it!

=head1 NAME

Genezzo::GenDBI.pm - an extensible database with SQL and DBI

=head1 SYNOPSIS

  # Basic line-mode usage
  use Genezzo::GenDBI; # see gendba.pl

  my $fb = Genezzo::GenDBI->new(exe => $0, 
                                gnz_home => $mygnz_home, 
                                dbinit => $do_init);

  $fb->Parseall($myquery); # process a statement

  $fb->Interactive();      # invoke line mode

  # DBI-style usage - see perldoc DBI, <http://dbi.perl.org/>
  my $dbh = Genezzo::GenDBI->connect($mygnz_home);
  my $rv  = Genezzo::GenDBI->do("startup");
  
  my @row_ary  = $dbh->selectrow_array($statement);
  my $ary_ref  = $dbh->selectrow_arrayref($statement);
  my $hash_ref = $dbh->selectrow_hashref($statement);

  my $sth = $dbh->prepare($statement);
  $rv     = $sth->execute;

  @row_ary  = $sth->fetchrow_array;
  $ary_ref  = $sth->fetchrow_arrayref;
  $hash_ref = $sth->fetchrow_hashref;

  $rv  = $sth->rows;
  $rv  = Genezzo::GenDBI->do("commit");
  $rv  = Genezzo::GenDBI->do("shutdown");

=head1 DESCRIPTION

  The Genezzo modules implement a hierarchy of persistent hashes using
  a fixed amount of memory and disk.  This system is designed to be
  easily configured and extended with custom functions, persistent
  storage representations, and novel data access methods.  In its
  current incarnation it supports a subset of SQL and a partial
  DBI interface.

=head2 EXPORT

 VERSION, RELSTATUS, RELDATE: version, release status, and release date

=head1 TODO

=over 4

=item This module is a bit of a catch-all, since it contains a
DBI-style interface, an interactive loop with an interpreter and some
presentation code, plus some expression evaluation and query planning
logic.  It needs to get split up.  

=back

=head1 AUTHOR

Jeffrey I. Cohen, jcohen@genezzo.com

=head1 SEE ALSO

L<perl(1)>, C<gendba.pl -man>,
C<perldoc DBI>, L<http://dbi.perl.org/>

Copyright (c) 2003, 2004, 2005 Jeffrey I Cohen.  All rights reserved.

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
at L<http://www.genezzo.com>

=cut
