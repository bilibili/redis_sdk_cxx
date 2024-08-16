#ifndef RSDK_TEST_SSTREAM_DEFINE_AS_PUBLIC_H
#define RSDK_TEST_SSTREAM_DEFINE_AS_PUBLIC_H

#ifdef private

#undef private
#include <sstream>
#define private public

#else // private

#include <sstream>

#endif // private

#endif // RSDK_TEST_SSTREAM_DEFINE_AS_PUBLIC_H
