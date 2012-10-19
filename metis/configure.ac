dnl Process this file with autoconf to produce a configure script

AC_INIT([metis], [0.1])
AC_PREREQ(2.60)
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([GNUmakefile])
AC_SUBST(ac_configure_args)

dnl Memory allocator
AC_CHECK_LIB([flow], [malloc], [have_flow=true], [have_flow=])
AC_CHECK_LIB([jemalloc], [malloc], [have_jemalloc=true], [have_jemalloc=])

AC_ARG_WITH([malloc],
    [AS_HELP_STRING([--with-malloc=TYPE], [Memory allocator (malloc|flow|jemalloc)])],
    [ac_cv_malloc=$withval], [ac_cv_malloc=yes])

if test \( "$ac_cv_malloc" = flow -a -z "$have_flow" \) -o \
        \( "$ac_cv_malloc" = jemalloc -a -z "$have_jemalloc" \) ; then
    AC_MSG_ERROR([$ac_cv_malloc not found])
fi
if test "$ac_cv_malloc" = yes ; then
    MEM_ALLOCATOR=
else
    MEM_ALLOCATOR=-l$ac_cv_malloc
fi

AC_SUBST([MEM_ALLOCATOR])

dnl Configure metis mode
ac_cv_all_modes="metis single_btree single_append-reduce_first single_append-group_merge_first single_append-nogroup_merge_first"
AC_ARG_ENABLE([mode],
              [AS_HELP_STRING([--enable-mode=ARG],
                              [mode: $ac_cv_all_modes, default: metis])],
              [ac_cv_mode=$enableval], [ac_cv_mode=metis])

ac_cv_map_merge_reduce=false
ac_cv_force_append=false
if test "$ac_cv_mode" = single_btree ; then
    AC_DEFINE_UNQUOTED([SINGLE_BTREE], [1],
                       [Define if you want grouped-map -> Merge -> Reduce])
    ac_cv_map_merge_reduce=true
elif test "$ac_cv_mode" = single_append-reduce_first ; then
    AC_DEFINE_UNQUOTED([SINGLE_APPEND_REDUCE_FIRST], [1],
                       [Define if you want append-map -> Reduce -> Merge])
    ac_cv_force_append=true
elif test "$ac_cv_mode" = single_append-group_merge_first ; then
    AC_DEFINE_UNQUOTED([SINGLE_APPEND_GROUP_MERGE_FIRST], [1],
                       [Define if you want append-map -> group -> Merge -> Reduce])
    ac_cv_map_merge_reduce=true
    ac_cv_force_append=true
elif test "$ac_cv_mode" = single_append-nogroup_merge_first ; then
    AC_DEFINE_UNQUOTED([SINGLE_APPEND_NOGROUP_MERGE_FIRST], [1],
                       [Define if you want append-map -> Merge -> Reduce])
    ac_cv_map_merge_reduce=true
    ac_cv_force_append=true
fi

ac_cv_force_psrs=$ac_cv_map_merge_reduce
if test "$ac_cv_force_append" = true; then
    AC_DEFINE_UNQUOTED([FORCE_APPEND], [1],
                       [Forced to use append-only data buffer for map phase])
fi
if test "$ac_cv_map_merge_reduce" = true; then
    AC_DEFINE_UNQUOTED([MAP_MERGE_REDUCE], [1], [map -> merge -> reduce])
fi

dnl Sort algorithm
ac_cv_sort=psrs
AC_ARG_ENABLE([sort],
              [AS_HELP_STRING([--enable-sort=ARG],
                               [mode: psrs or mergesort, default: psrs])],
              [ac_cv_sort=$enableval], [ac_cv_sort=psrs])
if test "$ac_cv_sort" = psrs ; then
    AC_DEFINE_UNQUOTED([USE_PSRS], [1], [Define if you want to use psrs for sorting])
elif test "$ac_cv_force_psrs" = true ; then
    AC_MSG_ERROR([Metis must use psrs to sort in current mode])
else
    AC_DEFINE_UNQUOTED([USE_PSRS], [0], [Define if you want to use psrs for sorting])
fi

dnl Debug mode
AC_ARG_ENABLE([debug],
              [AS_HELP_STRING([--enable-debug],
                              [mode: -O0 in debug mode; -O3 otherwise, default: false])],
              [ac_cv_debug=true], [ac_cv_debug=false])

if test "$ac_cv_debug" = true ; then
    OPT_LEVEL=-O0
else
    OPT_LEVEL=-O3
fi

dnl Profiling
AC_ARG_ENABLE([profile],
              [AS_HELP_STRING([--enable-profile],
                              [profile: enable performmance counters (see lib/mr-prof.c), default: false])],
              [ac_cv_profile=true], [ac_cv_profile=false])

if test "$ac_cv_profile" = true ; then
    AC_DEFINE_UNQUOTED([PROFILE_ENABLED], [1],
                       [Define if you want metis to print out profiling results from performance counters])
fi

AC_SUBST([OPT_LEVEL])

AC_OUTPUT
