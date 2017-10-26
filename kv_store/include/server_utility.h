#ifndef __SERVER_UTILITY_H__
#define __SERVER_UTILITY_H__

#include <string>

using namespace std;

string alphabet("abcdefghijklmnopqrstuvwxyz");

string getNextDeviceID(string currentID) {
	char first = currentID.at(0);
	char second = currentID.at(1);
	if (second != 'z')
		return string(1, first) + string(1, alphabet.at(alphabet.find(second) + 1));
	else {
		if (first == 'b')
			return "ca";
		else
			return "error: name out of bound\n";
	}
}

#endif