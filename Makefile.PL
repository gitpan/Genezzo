use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'		=> 'Genezzo',
    'ABSTRACT'          => 'extensible database system',
    'AUTHOR'            => 'Jeffrey I Cohen (jcohen@cpan.org)',
#    'LICENSE'           => 'gpl',
    'VERSION_FROM'	=> 'lib/Genezzo/GenDBI.pm', # finds $VERSION
    'PREREQ_PM'		=> {

#        'SQL::Statement' => '0.1011', # now use feeble
        'Text::ParseWords' => '0.0',
        'Term::ReadLine'   => '0.0',
        'File::Path'       => '0.0',
        'File::Spec'       => '0.0',
        'IO::File'         => '0.0',
        'IO::Handle'       => '0.0'
#        'POSIX'            => '0.0'
    }, # e.g., Module::Name => 1.1

   'EXE_FILES' => [ 'lib/Genezzo/gendba.pl' ], # install in /usr/bin
);

package MY;
# Alter the Makefile to be more like Module::Build
# See ExtUtils::MM_Any
# From Chris Reinhardt NET::DNS

sub metafile {

    my $self    = shift;
    my $meta    = $self->SUPER::metafile_target(@_);
    my ($extra) = $self->echo('license: gpl', 'META.yml', 1);
    my ($e2)    = 
     $self->echo('abstract: an extensible database system (with SQL and DBI)',
                 'META.yml', 1);
    my ($e3) = 
     $self->echo('author: Jeffrey I Cohen (jcohen@cpan.org)',
                 'META.yml', 1);

    return "$meta\t$extra\n\t$e2\n\t$e3\n";

}
