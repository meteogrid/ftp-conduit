{-# LANGUAGE DeriveDataTypeable, OverloadedStrings #-}

-- | This module contains code to use files on a remote FTP server as
--   Sources and Sinks.
--
--   Using these functions looks like this:
--
-- > let uri = fromJust $ parseURI "ftp://ftp.kernel.org/pub/README_ABOUT_BZ2_FILES"
-- > runResourceT $ createSource uri $$ consume

module Network.FTP.Conduit
  ( createSink
  , createSource
  , createSink'
  , createSource'
  , FTPException(..)
  ) where

import Data.Conduit hiding (connect)
import Data.Conduit.Network (sinkSocket, sourceSocket)
import Data.Monoid ((<>))
import Control.Monad (void, liftM, when)
import Control.Monad.Trans.Resource (MonadResource, allocate, release)
import Control.Monad.IO.Class (MonadIO(liftIO))
import qualified Data.ByteString as BS
import Data.ByteString.UTF8 hiding (foldl)
import Network.Socket hiding (send, sendTo, recv, recvFrom, Closed)
import Network.BSD
import Network.Socket.ByteString
import Network.URI
import Control.Exception (Exception, throwIO)
import Data.Word
import Data.Bits
import Prelude hiding (getLine)
import Data.Typeable (Typeable)

-- | Thrown if a FTP-level protocol exception happens
data FTPException = UnexpectedCode Int BS.ByteString
                  | GeneralError String
                  | IncorrectScheme String
                  | SocketClosed
  deriving (Typeable, Show)

instance Exception FTPException

throw' :: (MonadIO m, Exception e) => e -> m a 
throw' = liftIO . throwIO

getByte :: MonadIO m => Socket -> m Word8
getByte s = do
  b <- liftIO (recv s 1)
  if BS.null b then throw' SocketClosed else return $ BS.head b

getLine :: MonadIO m => Socket -> m BS.ByteString
getLine s = do
  b <- getByte s
  helper b
  where helper b = do
          b' <- getByte s
          if BS.pack [b, b'] == "\r\n"
            then return BS.empty
            else helper b' >>= return . (BS.cons b)

extractCode :: BS.ByteString -> Int
extractCode = read . toString . (BS.takeWhile (/= 32))

readExpected :: MonadIO m => Socket -> Int -> m BS.ByteString
readExpected s i = do
  line <- getLine s
  --putStrLn $ "Read: " ++ (toString line)
  if extractCode line /= i
    then throw' $ UnexpectedCode i line
    else return line

readExpected_ :: MonadIO m => Socket -> Int -> m ()
readExpected_ s = void . readExpected s

writeLine :: MonadIO m => Socket -> BS.ByteString -> m ()
writeLine s bs = do
  --putStrLn $ "Writing: " ++ (toString bs)
  -- hardcode the newline for platform independence
  liftIO (sendAll s (bs <> "\r\n"))


-- | Create a conduit source out of a 'URI'. Uses the @RETR@ command.
createSource :: MonadResource m => URI -> Source m BS.ByteString
createSource = createSource' (const (return ()))

createSource'
  :: MonadResource m
  => (Socket -> IO ())
  -> URI
  -> Source m BS.ByteString
createSource' = connectFTP $ \(c, d, path) -> do
  writeLine c "TYPE I"
  readExpected_ c 200
  writeLine c ("RETR " <> path)
  readExpected_ c 150
  sourceSocket d

-- | Create a conduit sink out of a 'URI'. Uses the @STOR@ command.
createSink :: MonadResource m => URI -> Sink BS.ByteString m ()
createSink = createSink' (const (return ()))

createSink'
  :: MonadResource m
  => (Socket -> IO ()) -> URI -> Sink BS.ByteString m ()
createSink' = connectFTP $ \(c, d, path) -> do
  writeLine c "TYPE I"
  readExpected_ c 200
  writeLine c ("STOR " <> path)
  readExpected_ c 150
  sinkSocket d
  
connectFTP
  :: MonadResource m
  => ((Socket, Socket, BS.ByteString) -> ConduitM i o m r)
  -> (Socket -> IO ())
  -> URI
  -> ConduitM i o m r
connectFTP next sckCfg uri = do
  when (scheme' /= "ftp:") 
    (throw' (IncorrectScheme scheme'))
  (rc,c) <- connectTCP chost (fromIntegral cport)
  readExpected_ c 220
  writeLine c ("USER " <> fromString user)
  readExpected_ c 331
  writeLine c ("PASS " <> fromString pass)
  readExpected_ c 230
  writeLine c "TYPE I"
  readExpected_ c 200
  writeLine c "PASV"
  (pasvhost, pasvport) <- liftM parsePasvString (readExpected c 227)
    --putStrLn "Opening data connection"
  (rd,d) <- connectTCP (toString pasvhost) (fromIntegral pasvport)
  let closeConnection _ = do
        release rd 
        readExpected_ c 226
        writeLine c "QUIT"
        readExpected_ c 221
        release rc
  addCleanup closeConnection (next (c, d, fromString path'))
  where
    cport :: Word16
    (chost, cport, user, pass) = case authority' of
      Nothing -> undefined
      Just (URIAuth userInfo regName port') ->
        ( regName
        , if null port' then 21 else read (tail port')
        , if null userInfo then "anonymous" else takeWhile (\ l -> l /= ':' && l /= '@') userInfo
        , if null userInfo || not (':' `elem` userInfo) then "" else init $ tail $ (dropWhile (/= ':')) userInfo
        )
    parsePasvString ps = (pasvhost, pasvport)
      where pasvhost = BS.init $ foldl (\ a ip -> a <> (fromString $ show ip) <> ".") BS.empty [ip1, ip2, ip3, ip4]
            pasvport :: Word16
            pasvport = (fromIntegral port1) `shiftL` 8 + fromIntegral port2
            (ip1, ip2, ip3, ip4, port1, port2) = read $ toString $ (<> ")") $ (BS.takeWhile (/= 41)) $ (BS.dropWhile (/= 40)) ps :: (Int, Int, Int, Int, Int, Int)
    connectTCP host port = do
       he <- liftIO (getHostByName host)
       let addr = SockAddrInet port (hostAddress he)
       proto <- liftIO (getProtocolNumber "tcp")
       rs@(_,s) <- allocate (socket AF_INET Stream proto) close
       liftIO (sckCfg s >> connect s addr)
       return rs
    URI {uriScheme = scheme' , uriAuthority = authority' , uriPath = path' } = uri
