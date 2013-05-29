#include <iostream>
#include <fstream>
#include <sstream>
#include <stdint.h>
#include <vector>
#include <algorithm>

using namespace std;

static const int BLOCK_SIZE = 256 * 1024 * 1024; // 256 Mb

struct BufferRef
{
    int offset;
    int length;
};

struct CompareRefs
{
    CompareRefs(const char *buf) : buffer(buf) {}

    // a < b
    bool operator() (const BufferRef &a, const BufferRef &b)
    {
        if (a.length != b.length)
            return a.length < b.length;

        for( int i = 0; i < a.length; ++i )
        {
            char c1 = buffer[a.offset + i];
            char c2 = buffer[b.offset + i];

            if ( c1 == c2 )
                continue;

            return c1 < c2;
        }

        return false;
    }

private:
    const char *buffer;
};

class Sorter
{
public:
    Sorter( istream &is_, ostream &os_, char *buffer_ ) : is(is_), os(os_), buffer(buffer_)
    {
    }

    void Sort()
    {
        uint64_t file_offset = 0;
        uint64_t tail_reminder;
        int chunk = 0;

        while( is.good() )
        {
            is.seekg( file_offset, is.beg );
            is.read( buffer, BLOCK_SIZE );
            cout << "chunk " << chunk << " readed" << endl;

            tail_reminder = ParseChunk();
            cout << "chunk " << chunk << " parsed" << endl;
            file_offset += BLOCK_SIZE - tail_reminder;

            SortRefs();
            cout << "chunk " << chunk << " sorted" << endl;

            SaveSortedChunk( chunk );
            cout << "chunk " << chunk << " saved" << endl;

            ++chunk;
        }
    }

    uint64_t ParseChunk()
    {
        int i, offset;
        BufferRef ref;

        refs.clear();

        offset = 0;
        for( i = 0; i < BLOCK_SIZE; ++i )
        {
            if ( buffer[i] == '\n' )
            {
                ref.length = i - offset;
                ref.offset = offset;
                offset = i + 1;
                refs.push_back( ref );
            }
        }

        return (uint64_t)(BLOCK_SIZE - i);
    }

    void SortRefs()
    {
        CompareRefs cmp( buffer );
        std::sort( refs.begin(), refs.end(), cmp );
    }
    
    void SaveSortedChunk( int chunk )
    {
        ostringstream ss;
        ss << "chunk/" << chunk;
        ofstream o( ss.str().c_str() );

        for( int i = 0; i < refs.size(); ++i )
        {
            o.write( buffer + refs[i].offset, refs[i].length );
            o << endl;
        }
    }

protected:
    istream &is;
    ostream &os;
    char *buffer;
    vector<BufferRef> refs;
};


int main()
{
    cout << "sorting started..." << endl;

    char *buffer = new(std::nothrow) char[ BLOCK_SIZE ];
    if (!buffer)
    {
        cout << "malloc failed" << endl;
        return 1;
    }

    ifstream input( "input.txt" );
    ofstream output( "output.txt" );

    Sorter sorter( input, output, buffer );
    sorter.Sort();

    delete[] buffer;
    buffer = NULL;

    cout << "sorting finished..." << endl;
    return 0;
}
