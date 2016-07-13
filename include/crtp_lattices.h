// using namespace std;

// http://stackoverflow.com/questions/11546478/how-do-i-pass-template-parameters-to-a-crtp
template<typename Derived>
class enable_down_cast
{
private:
        // typedefs

        typedef enable_down_cast Base;

public:
        Derived const* self() const
        {
                // casting "down" the inheritance hierarchy
                return static_cast<Derived const*>(this);
        }

        // write the non-const version in terms of the const version
        // Effective C++ 3rd ed., Item 3 (p. 24-25)
        Derived* self()
        {
                return const_cast<Derived*>(static_cast<Base const*>(this)->self());
        }

protected:
        // disable deletion of Derived* through Base*
        // enable deletion of Base* through Derived*
        ~enable_down_cast() = default; // C++11 only, use ~enable_down_cast() {} in C++98
};

template <typename T>
class LatticeMixin : public enable_down_cast<T> {
 private:
 	using enable_down_cast< T >::self;

 public:
	LatticeMixin<T>() {
		self()->assign(self()->bot);
	}
	LatticeMixin<T>(const T &e) {
		self()->assign(e);
	}
	LatticeMixin<T>(const LatticeMixin<T> &other) {
		self()->assign(other.self()->reveal());
	}
	~LatticeMixin<T> () = default;
	LatticeMixin<T>& operator=(const LatticeMixin<T> &rhs) {
        self()->assign(rhs.self()->reveal());
        return *this;
    }
    bool operator==(const LatticeMixin<T>& rhs) const {
		return self()->reveal() == rhs.self()->reveal();
	}
	void merge(const T &e) {
		return self()->do_merge(e.reveal());
	}
	size_t hash(const T &e) const {
		return self()->hash(e.reveal());
	}
};

#ifdef CRTP_COLLECTIONS_WORKING
// From: http://stackoverflow.com/questions/1032973/how-to-partially-specialize-a-class-template-for-all-derived-types

// template<typename T> struct X; 
namespace std { namespace __1 {
    template<class T>
    struct hash< unordered_set<T> > {
    	size_t operator()(const LatticeMixin< unordered_set<T> > & base) const     
    	{         
    		return base.hash();     
    	}
    };
}} // namespace __1 // namespace std 
#endif