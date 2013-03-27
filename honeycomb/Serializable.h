#ifndef SERIALIZABLE_H

#define SERIALIZABLE_H

class Serializable
{
  public:
    virtual ~Serializable(){} 
    virtual int serialize(const char** buf, size_t* len) = 0;

    virtual int deserialize(const char* buf, int64_t len) = 0;
};

#endif 
