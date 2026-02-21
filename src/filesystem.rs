use std::collections::{BTreeSet, VecDeque};
use std::ffi::{OsStr, OsString};
use std::os::unix::fs::FileTypeExt as _;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::RwLock;

pub const FUSE_ROOT_ID: u64 = 1;

/// File types
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FileType {
    /// Named pipe (`S_IFIFO`)
    NamedPipe,
    /// Character device (`S_IFCHR`)
    CharDevice,
    /// Block device (`S_IFBLK`)
    BlockDevice,
    /// Directory (`S_IFDIR`)
    Directory,
    /// Regular file (`S_IFREG`)
    RegularFile,
    /// Symbolic link (`S_IFLNK`)
    Symlink,
    /// Unix domain socket (`S_IFSOCK`)
    Socket,
}

impl FileType {
    /// Convert std `FileType` to fuser `FileType`.
    pub fn from_std(file_type: std::fs::FileType) -> Option<Self> {
        if file_type.is_file() {
            Some(FileType::RegularFile)
        } else if file_type.is_dir() {
            Some(FileType::Directory)
        } else if file_type.is_symlink() {
            Some(FileType::Symlink)
        } else if file_type.is_fifo() {
            Some(FileType::NamedPipe)
        } else if file_type.is_socket() {
            Some(FileType::Socket)
        } else if file_type.is_char_device() {
            Some(FileType::CharDevice)
        } else if file_type.is_block_device() {
            Some(FileType::BlockDevice)
        } else {
            None
        }
    }
}

/// File attributes
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileAttr {
    /// Inode number
    pub ino: u64,
    /// Size in bytes
    pub size: u64,
    /// Allocated size in 512-byte blocks. May be smaller than the actual file size
    /// if the file is compressed, for example.
    pub blocks: u64,
    /// Time of last access
    pub atime: SystemTime,
    /// Time of last modification
    pub mtime: SystemTime,
    /// Time of last change
    pub ctime: SystemTime,
    /// Time of creation (macOS only)
    pub crtime: SystemTime,
    /// Kind of file (directory, file, pipe, etc)
    pub kind: FileType,
    /// Permissions
    pub perm: u16,
    /// Number of hard links
    pub nlink: u32,
    /// User id
    pub uid: u32,
    /// Group id
    pub gid: u32,
    /// Rdev
    pub rdev: u32,
    /// Block size to be reported by `stat()`. If unsure, set to 4096.
    pub blksize: u32,
    /// Flags (macOS only, see chflags(2))
    pub flags: u32,
}

/// Directory entry (returned by [`Filesystem::readdir`]).
#[derive(Debug)]
pub struct DirEntry {
    pub inode: u64,
    pub file_type: FileType,
    pub name: String, // TODO: Use OsStr and/or Cow
}
impl DirEntry {
    pub fn path(&self) -> &Path {
        Path::new(&self.name)
    }
}

/// Base trait providing asynchronous functions for the system calls.
///
/// WARNING: The [`crate::FilesystemFUSE`] struct encapsulates implementors in a [`RwLock`].
///          Therefore, care should be taken to not introduce deadlocks.
///          An example would be if `open` waits on a `release` (e.g. to limit the number of open
///          files), but the `open` acquires a write lock before any `release`.
#[async_trait::async_trait]
pub trait Filesystem {
    type Error: Into<crate::error::Error>;
    /// Close filesystem. See also [`Filesystem::destroy`].
    async fn destroy(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    /// Lookup an entry by name in a directory. See also [`Filesystem::lookup`].
    async fn lookup(&self, parent: u64, name: &OsStr) -> Result<FileAttr, Self::Error>;
    /// Open a file and get a handle. See also [`Filesystem::open`].
    async fn open(&self, _ino: u64, _flags: i32) -> Result<u64, Self::Error> {
        Ok(0)
    }
    /// Release a file handle. See also [`Filesystem::release`].
    async fn release(&self, _ino: u64, _fh: u64) -> Result<(), Self::Error> {
        Ok(())
    }
    /// Get attributes on an entry. See also [`Filesystem::getattr`].
    async fn getattr(&self, _ino: u64) -> Result<FileAttr, Self::Error>;
    /// Set attributes. Currently only supports setting the size
    async fn setattr(
        &mut self,
        ino: u64,
        size: Option<u64>,
    ) -> Result<FileAttr, Self::Error>;
    /// Read a directory.
    /// To be called repeatedly by specifying a gradually increasing offset,
    /// until the returned iterator is empty. A minimum of two calls is required
    /// to be certain that the end has been reached.
    /// `offset` represents the index of the starting element.
    /// See also [`Filesystem::readdir`].
    async fn readdir(
        &self,
        ino: u64,
        offset: u64,
    ) -> Result<Box<dyn Iterator<Item = DirEntry> + Send + Sync + '_>, Self::Error>;
    /// Read from a file. See also [`Filesystem::read`].
    async fn read(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
    ) -> Result<bytes::Bytes, Self::Error>;
    /// Write to file. See also [`Filesystem::write`].
    async fn write(
        &self,
        ino: u64,
        fh: u64,
        data: bytes::Bytes,
        offset: i64,
    ) -> Result<u32, Self::Error>;
    /// Create and open a file. See also [`Filesystem::create`].
    async fn create(
        &mut self,
        parent: u64,
        name: OsString,
        mode: u32,
        umask: u32,
        flags: i32,
    ) -> Result<(FileAttr, u64), Self::Error>;
    /// Create a directory. See also [`Filesystem::mkdir`].
    async fn mkdir(&mut self, parent: u64, name: OsString) -> Result<FileAttr, Self::Error>;
    /// Get the set of inodes from the filesystem.
    ///
    /// Implementors can usually provide a more efficient implementation than the blanket one,
    /// which performs BFS from the root.
    async fn inodes(&self) -> Result<BTreeSet<u64>, Self::Error> {
        let mut inodes = BTreeSet::from([FUSE_ROOT_ID]);
        let mut queue = VecDeque::from([FUSE_ROOT_ID]);
        while let Some(ino) = queue.pop_front() {
            for entry in self.readdir(ino, 0).await? {
                inodes.insert(entry.inode);
                if entry.file_type == FileType::Directory {
                    queue.push_back(entry.inode);
                }
            }
        }
        Ok(inodes)
    }
}

#[async_trait::async_trait]
impl<T: Filesystem + Send + Sync + 'static> Filesystem for Arc<RwLock<T>> {
    type Error = T::Error;
    async fn destroy(&mut self) -> Result<(), Self::Error> {
        let mut x = self.as_ref().write().await;
        x.destroy().await
    }
    async fn lookup(&self, parent: u64, name: &OsStr) -> Result<FileAttr, Self::Error> {
        let x = self.as_ref().read().await;
        x.lookup(parent, name).await
    }
    async fn open(&self, ino: u64, flags: i32) -> Result<u64, Self::Error> {
        let x = self.as_ref().read().await;
        x.open(ino, flags).await
    }
    async fn release(&self, ino: u64, fh: u64) -> Result<(), Self::Error> {
        let x = self.as_ref().read().await;
        x.release(ino, fh).await
    }
    async fn getattr(&self, ino: u64) -> Result<FileAttr, Self::Error> {
        let x = self.as_ref().read().await;
        x.getattr(ino).await
    }
    async fn setattr(
        &mut self,
        ino: u64,
        size: Option<u64>,
    ) -> Result<FileAttr, Self::Error> {
        let mut x = self.as_ref().write().await;
        x.setattr(ino, size).await
    }
    async fn readdir(
        &self,
        ino: u64,
        offset: u64,
    ) -> Result<Box<dyn Iterator<Item = DirEntry> + Send + Sync + '_>, Self::Error> {
        let x = self.as_ref().read().await;
        let dir = x.readdir(ino, offset).await?;
        // TODO: Avoid the collect (that would likely mean returning a `Stream` instead of an `Iterator`)
        Ok(Box::new(dir.collect::<Vec<_>>().into_iter()))
    }
    async fn read(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
    ) -> Result<bytes::Bytes, Self::Error> {
        let x = self.as_ref().read().await;
        x.read(ino, fh, offset, size).await
    }
    async fn write(
        &self,
        ino: u64,
        fh: u64,
        data: bytes::Bytes,
        offset: i64,
    ) -> Result<u32, Self::Error> {
        let x = self.as_ref().read().await;
        x.write(ino, fh, data, offset).await
    }
    async fn create(
        &mut self,
        parent: u64,
        name: OsString,
        mode: u32,
        umask: u32,
        flags: i32,
    ) -> Result<(FileAttr, u64), Self::Error> {
        let mut x = self.as_ref().write().await;
        x.create(parent, name, mode, umask, flags).await
    }
    async fn mkdir(&mut self, parent: u64, name: OsString) -> Result<FileAttr, Self::Error> {
        let mut x = self.as_ref().write().await;
        x.mkdir(parent, name).await
    }
    async fn inodes(&self) -> Result<BTreeSet<u64>, Self::Error> {
        let x = self.as_ref().read().await;
        x.inodes().await
    }
}
