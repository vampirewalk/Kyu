//
//  KyuJob.swift
//  Kyu
//
//  Created by Red Davis on 09/01/2016.
//  Copyright © 2016 Red Davis. All rights reserved.
//

import Foundation


internal enum KyuJobError: Error
{
    case jsonFileNotFound
    case invalidJSON
}


internal final class KyuJob
{
    fileprivate static let JSONFileName = "JSON"
    
    /**
     Use this method to create a job. It firstly creates the jobs directory structure
     in a temporary directory and then moves it to the queue directory specified
     
     - parameter arguments:    Job arguments
     - parameter queueDirectoryURL: The queue directory URL
     */
    internal class func createJob(_ identifier: String, arguments: [String : AnyObject], queueDirectoryURL: URL)
    {
        let fileManager = FileManager.default
        
        // Temporary directory
        let temporaryDirectory = URL(string: NSTemporaryDirectory())!
        let kyuTemporaryDirectory = temporaryDirectory.appendingPathComponent("KyuTemp")
        
        // Create job in temporary directory
        let jobTemporaryDirectoryURL = kyuTemporaryDirectory.appendingPathComponent(identifier)
        try! fileManager.createDirectory(atPath: jobTemporaryDirectoryURL.path, withIntermediateDirectories: true, attributes: nil)
        
        // Write JSON
        let JSONFileURL = jobTemporaryDirectoryURL.appendingPathComponent(KyuJob.JSONFileName)
        
        let JSONData = try! JSONSerialization.data(withJSONObject: arguments, options: [])
        try? JSONData.write(to: URL(fileURLWithPath: JSONFileURL.path), options: [.atomic])
        
        // Move Job to the queue directory
        let jobDirectoryURL = queueDirectoryURL.appendingPathComponent(identifier)
        try! fileManager.moveItem(atPath: jobTemporaryDirectoryURL.path, toPath: jobDirectoryURL.path)
    }
    
    // Internal
    internal let JSON: [String : AnyObject]
    
    internal var processDate: Date {
        guard let directoryAttributes = try? fileManager.attributesOfItem(atPath: self.directoryURL.path),
            let modifiedDate = directoryAttributes[FileAttributeKey.modificationDate] as? Date else
        {
            return Date()
        }
        
        return modifiedDate
    }
    
    internal var shouldProcess: Bool {
        let nowDate = Date()
        return self.processDate.compare(nowDate) == ComparisonResult.orderedAscending
    }
    
    internal var numberOfRetries: Int {
        var numberOfRetries = 0
        do
        {
            let retries = try self.fileManager.contentsOfDirectory(atPath: self.retryAttemptDirectoryURL.path)
            numberOfRetries = retries.count
        }
        catch { }
        
        return numberOfRetries
    }
    
    internal var identifier: String {
        return self.directoryURL.lastPathComponent
    }
    
    // Private
    fileprivate let fileManager = FileManager.default
    fileprivate let directoryURL: URL
    
    fileprivate let retryAttemptDirectoryName = "retries"
    fileprivate let retryAttemptDirectoryURL: URL
    
    // MARK: Initialization
    
    internal required init(directoryURL: URL) throws
    {
        self.directoryURL = directoryURL
        self.retryAttemptDirectoryURL = self.directoryURL.appendingPathComponent(self.retryAttemptDirectoryName, isDirectory: true)
        
        let JSONURL = directoryURL.appendingPathComponent(KyuJob.JSONFileName)
        let JSONURLPath = JSONURL.path
        
        guard let JSONData = try? Data(contentsOf: URL(fileURLWithPath: JSONURLPath)) else
        {
            throw KyuJobError.jsonFileNotFound
        }
        
        guard let JSON = (try JSONSerialization.jsonObject(with: JSONData, options: [])) as? [String : AnyObject] else
        {
            throw KyuJobError.invalidJSON
        }
        
        self.JSON = JSON
    }
    
    // MARK: -
    
    internal func delete()
    {
        do
        {
            try self.fileManager.removeItem(atPath: self.directoryURL.path)
        }
        catch
        {
            // Directory has been deleted?
        }
    }
    
    // MARK: Retries
    
    internal func incrementRetryCount()
    {
        if !self.retryDirectoryExists()
        {
            do
            {
                try self.createRetryDirectory()
            }
            catch
            {
                
            }
        }
        
        let retryFileName = UUID().uuidString
        let retryFileURL = self.retryAttemptDirectoryURL.appendingPathComponent(retryFileName)
        self.fileManager.createFile(atPath: retryFileURL.path, contents: nil, attributes: nil)
        
        self.setNextRetryDate()
    }
    
    fileprivate func setNextRetryDate()
    {
        do
        {
            let numberOfRetries = Double(self.numberOfRetries)
            let delta = 3.0
            let seconds = 30.0
            
            let retryTimeInterval = seconds * pow(numberOfRetries, delta)
            let retryDate = Date().addingTimeInterval(retryTimeInterval)
            
            let attributes = [FileAttributeKey.modificationDate: retryDate]
            
            let directoryPath = self.directoryURL.path
            try self.fileManager.setAttributes(attributes, ofItemAtPath: directoryPath)
        }
        catch
        {
            // TODO: ?
        }
    }
    
    // MARK: -
    
    fileprivate func retryDirectoryExists() -> Bool
    {
        return self.fileManager.fileExists(atPath: self.retryAttemptDirectoryURL.path)
    }
    
    fileprivate func createRetryDirectory() throws
    {
        try self.fileManager.createDirectory(atPath: self.retryAttemptDirectoryURL.path, withIntermediateDirectories: true, attributes: nil)
    }
}
