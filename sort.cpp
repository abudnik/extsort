#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <stdint.h>
#include <vector>
#include <algorithm>

using namespace std;

static const int BLOCK_SIZE = 256 * 1024 * 1024; // 256 Mb
static const int OUTPUT_BUFFER_SIZE = 32 * 1024 * 1024; // 32 Mb


struct Merger
{
    virtual ~Merger() {}
    virtual void MergeChunks( int chunk ) = 0;
};

class Sorter
{
    struct ChunkRef
    {
        char *data;
        int length;
    };

    struct CompareRefs
    {
        // a < b
        bool operator() ( const ChunkRef &a, const ChunkRef &b ) const
        {
            if ( a.length != b.length )
                return a.length < b.length;

            return strncmp( a.data, b.data, a.length ) < 0;
        }
    };

public:
    Sorter( istream &is, char *buffer, Merger *merger )
    : is_( is ),
     buffer_( buffer ),
     merger_( merger )
    {}

    void Sort()
    {
        int chunk = SortChunks();
        merger_->MergeChunks( chunk );
    }

private:
    int SortChunks()
    {
        uint64_t file_offset = 0;
        int tail_reminder;
        int chunk = 0;

        while( is_.good() )
        {
            is_.seekg( file_offset, is_.beg );
            is_.read( buffer_, BLOCK_SIZE );
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

        return chunk;
    }

    int ParseChunk()
    {
        int i, offset;
        ChunkRef ref;

        refs_.clear();

        offset = 0;
        for( i = 0; i < BLOCK_SIZE; ++i )
        {
            if ( buffer_[i] == '\n' )
            {
                ref.length = i - offset;
                ref.data = buffer_ + offset;
                offset = i + 1;
                refs_.push_back( ref );
            }
        }

        // get uncomplete number length in buffer (reminder)
        if ( buffer_[BLOCK_SIZE - 1] != '\n' )
        {
            i = offset + refs_.back().length + 1;
            return BLOCK_SIZE - i;
        }

        return 0;
    }

    void SortRefs()
    {
        std::sort( refs_.begin(), refs_.end(), CompareRefs() );
    }
    
    void SaveSortedChunk( int chunk ) const
    {
        ostringstream ss;
        ss << "chunk/" << chunk;
        ofstream o( ss.str().c_str() );

        for( size_t i = 0; i < refs_.size(); ++i )
        {
            o.write( refs_[i].data, refs_[i].length );
            o << endl;
        }
    }

protected:
    istream &is_;
    char *buffer_;
    Merger *merger_;
    vector< ChunkRef > refs_;
};

class MultiPhaseMerger : public Merger
{
    struct BufferRef
    {
        int length;
        char *data;
        int bufferIndex;
    };

    struct MergeBuffer
    {
        char *buffer;
        vector<BufferRef> refs;
        int current_ref;
        uint64_t file_offset;
    };

    // a < b
    struct CmpLess
    {
        bool operator ()( const BufferRef &a, const BufferRef &b ) const
        {
            if ( a.length != b.length )
                return a.length < b.length;

            return strncmp( a.data, b.data, a.length ) < 0;
        }
    };

public:
    MultiPhaseMerger( ostream &os, char *buffer )
    : os_( os ),
     buffer_( buffer ),
     output_buffer_( new char[ OUTPUT_BUFFER_SIZE ] ),
     output_offset_( 0 )
    {}

    ~MultiPhaseMerger()
    {
        delete[] output_buffer_;
    }

private:
    virtual void MergeChunks( int chunk )
    {
        InitMergeBuffers( chunk );

        min_heap_.resize( chunk );
        for( int i = 0; i < chunk; ++i )
        {
            FillEmptyBuffer( i );
            min_heap_[i] = buffers_[i].refs[0];
        }
        make_heap( min_heap_.begin(), min_heap_.end(), CmpLess() );
        heapSize_ = min_heap_.size();

        BufferRef min;
        int buffer;

        while( heapSize_ )
        {
            buffer = FindMinimumValue( min );
            if ( FillEmptyBuffer( buffer ) )
                PushHeap( buffer );
            PushToOutput( min );
        }

        FlushOutput();
    }

    int FindMinimumValue( BufferRef &min )
    {
        pop_heap( min_heap_.begin(), min_heap_.begin() + heapSize_, CmpLess() );
        min = min_heap_[ --heapSize_ ];

        int buffer = min.bufferIndex;

        ++buffers_[ buffer ].current_ref;
        return buffer;
    }

    void PushHeap( int buffer )
    {
        int current_ref = buffers_[ buffer ].current_ref;
        min_heap_[ heapSize_++ ] = buffers_[ buffer ].refs[ current_ref ];
        push_heap( min_heap_.begin(), min_heap_.begin() + heapSize_, CmpLess() );
    }

    void PushToOutput( const BufferRef &min )
    {
        if ( output_offset_ + min.length + 1 >= OUTPUT_BUFFER_SIZE )
        {
            // flush buffer to disk
            os_.write( output_buffer_, output_offset_ );
            output_offset_ = 0;
        }

        strncpy( output_buffer_ + output_offset_, min.data, min.length );
        output_buffer_[ output_offset_ + min.length ] = '\n';
        output_offset_ += min.length + 1;
    }

    void FlushOutput()
    {
        if ( output_offset_ )
        {
            os_.write( output_buffer_, output_offset_ );
        }
    }

    bool FillEmptyBuffer( int buffer )
    {
        const int i = buffer;
        if ( buffers_[i].current_ref >= buffers_[i].refs.size() ) // check if buffer is empty
        {
            if ( buffers_[i].file_offset >= BLOCK_SIZE ) // chunk were parsed already
                return false;

            ostringstream ss;
            ss << "chunk/" << i;
            ifstream is( ss.str().c_str() );

            is.seekg( buffers_[i].file_offset, is.beg );
            is.read( buffers_[i].buffer, buffer_size_ );

            int tail_reminder = ParseBuffer( i );
            buffers_[i].file_offset += buffer_size_ - tail_reminder;

            cout << "read buffer of chunk = " << i << endl;
        }
        return true;
    }

    int ParseBuffer( int buffer )
    {
        MergeBuffer &b = buffers_[ buffer ];

        int i, offset;
        BufferRef ref;
        ref.bufferIndex = buffer;

        b.refs.clear();
        b.current_ref = 0;

        offset = 0;
        for( i = 0; i < buffer_size_; ++i )
        {
            if ( b.buffer[i] == '\n' )
            {
                ref.length = i - offset;
                ref.data = b.buffer + offset;
                offset = i + 1;
                b.refs.push_back( ref );
            }
        }

        if ( b.buffer[buffer_size_ - 1] != '\n' )
        {
            i = offset + b.refs.back().length + 1;
            return buffer_size_ - i;
        }

        return 0;
    }

    void InitMergeBuffers( int num_buffers )
    {
        // use merge buffers on top of buffer
        buffers_.resize( num_buffers );
        buffer_size_ = BLOCK_SIZE / num_buffers;

        for( int i = 0; i < num_buffers; ++i )
        {
            buffers_[i].buffer = buffer_ + i * buffer_size_;
            buffers_[i].file_offset = 0;
        }
    }

private:
    ostream &os_;
    char *buffer_;
    std::vector< MergeBuffer > buffers_; // small buffers for each chunk
    int buffer_size_;
    char *output_buffer_;
    int output_offset_;
    std::vector< BufferRef > min_heap_;
    size_t heapSize_;
};


int main()
{
    cout << "sorting started..." << endl;

    char *buffer = new(std::nothrow) char[ BLOCK_SIZE ];
    if ( !buffer )
    {
        cout << "malloc failed" << endl;
        return 1;
    }

    ifstream input( "input.txt" );
    ofstream output( "output.txt" );

    MultiPhaseMerger merger( output, buffer );
    Sorter sorter( input, buffer, &merger );
    sorter.Sort();

    delete[] buffer;

    cout << "sorting finished..." << endl;
    return 0;
}
