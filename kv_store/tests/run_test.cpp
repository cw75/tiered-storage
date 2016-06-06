#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "test_KVS.h"

int main (int argc, char *argv[])
{
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}