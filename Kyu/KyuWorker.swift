//
//  KyuThread.swift
//  Kyu
//
//  Created by Red Davis on 14/11/2015.
//  Copyright © 2015 Red Davis. All rights reserved.
//

import Foundation


internal protocol KyuWorkerDataSource
{
    func workerShouldIncrementRetryCounts() -> Bool
    func jobForKyuWorker(_ worker: KyuWorker) -> KyuJobProtocol
    func baseTemporaryDirectoryForKyuWorker(_ worker: KyuWorker) -> URL
    func baseJobDirectoryForKyuWorker(_ worker: KyuWorker) -> URL
    func maximumNumberOfRetriesForKyuWorker(_ worker: KyuWorker) -> Int
}


internal protocol KyuWorkerDelegate
{
    func worker(_ worker: KyuWorker, didStartProcessingJob job: KyuJob)
    func worker(_ worker: KyuWorker, didFinishProcessingJob job: KyuJob, withResult result: KyuJobResult)
}


/**
 KyuConfigurationError
 */
public enum KyuWorkerInitializationError: Error
{
    case errorCreatingWorkerDirectory
}


final internal class KyuWorker
{
    internal let identifier: String
    
    internal var delegate: KyuWorkerDelegate?
    
    internal var paused = false {
        didSet
        {
            if !self.paused
            {
                self.processNextJob()
            }
        }
    }
    
    internal var numberOfJobs: Int {
        let fileManager = FileManager.default
        
        var numberOfJobs = 0
        do
        {
            let jobs = try fileManager.contentsOfDirectory(atPath: self.queueDirectoryPathURL.path)
            numberOfJobs = jobs.count
        }
        catch { }
        
        return numberOfJobs
    }
    
    // Private
    fileprivate let dataSource: KyuWorkerDataSource
    
    // Queue management
    fileprivate var queueDirectoryPathURL: URL {
        let baseURL = self.dataSource.baseJobDirectoryForKyuWorker(self)
        return baseURL.appendingPathComponent(self.identifier)
    }
    
    fileprivate let checkJobsTimerQueue: DispatchQueue
    
    fileprivate let queueDirectoryObserverQueue: DispatchQueue
    fileprivate var queueDirectoryObserver: DispatchSource!
    fileprivate let queueDirectoryOperationQueue = OperationQueue()
    fileprivate var isProcessing = false
    
    fileprivate let jobProcessingOperationQueue: OperationQueue = {
        let operationQueue = OperationQueue()
        operationQueue.maxConcurrentOperationCount = 1
        
        return operationQueue
    }()
    
    fileprivate let jobFetchingOperationQueue = OperationQueue()
    
    fileprivate var currentJob: KyuJob?
    
    // JSON
    fileprivate let JSONFilename = "JSON"
    
    // Temporary directory
    fileprivate var temporaryDirectoryPathURL: URL {
        let baseTemporaryDirectoryURL = self.dataSource.baseTemporaryDirectoryForKyuWorker(self)
        return baseTemporaryDirectoryURL.appendingPathComponent(self.identifier)
    }
    
    // Workers
    fileprivate var worker: KyuJobProtocol {
        return self.dataSource.jobForKyuWorker(self)
    }
    
    // Retry
    fileprivate var maximumNumberOfRetries: Int {
        return self.dataSource.maximumNumberOfRetriesForKyuWorker(self)
    }
    
    // MARK: Initialization
    
    internal required init(identifier: String, dataSource: KyuWorkerDataSource) throws
    {
        self.identifier = identifier
        self.dataSource = dataSource
        
        self.checkJobsTimerQueue = DispatchQueue(label: "com.kyu.\(self.identifier)-check-jobs", attributes: [])
        self.queueDirectoryObserverQueue = DispatchQueue(label: "com.kyu.\(self.identifier)", attributes: [])
        
        // Create directories
        do
        {
            try self.setupTemporaryDirectory()
            try self.setupQueueDirectory()
        }
        catch
        {
            throw KyuWorkerInitializationError.errorCreatingWorkerDirectory
        }
        
        // Queue directory observer
        let directoryFileDescriptor = UInt(open((self.queueDirectoryPathURL as NSURL).fileSystemRepresentation, O_EVTONLY))
        self.queueDirectoryObserver = DispatchSource.makeFileSystemObjectSource(fileDescriptor: Int32(directoryFileDescriptor), eventMask: DispatchSource.FileSystemEvent.write, queue: self.queueDirectoryObserverQueue) /*Migrator FIXME: Use DispatchSourceFileSystemObject to avoid the cast*/ as! DispatchSource
        
        self.queueDirectoryObserver.setEventHandler(handler: { [weak self] () -> Void in
            if let weakSelf = self
            {
                weakSelf.queueDirectoryUpdated()
            }
        })
        
        self.queueDirectoryObserver.resume()
        
        // Check queue timer
        self.dispatchCheckJobsQueue()
        
        // Process next job
        self.processNextJob()
    }
    
    deinit
    {
    
    }
    
    // MARK: Job management
    
    internal func cancelJob(_ identifier: String) -> Bool
    {
        let job = self.fetchAllJobs().filter { (job) -> Bool in
            return job.identifier == identifier && self.currentJob?.identifier != job.identifier
        }.first
        
        guard let unwrappedJob = job else
        {
            return false
        }

        unwrappedJob.delete()
        return true
    }
    
    internal func queueJob(_ arguments: KyuJobArguments) -> String
    {
        let jobIdentifier = UUID().uuidString
        
        self.queueDirectoryOperationQueue.addOperation { () -> Void in
            // Create job
            KyuJob.createJob(jobIdentifier, arguments: arguments, queueDirectoryURL: self.queueDirectoryPathURL)
            self.processNextJob()
        }
        
        return jobIdentifier
    }
    
    fileprivate func processNextJob()
    {
        if self.isProcessing || self.paused
        {
            return
        }
        
        self.isProcessing = true
        
        self.jobProcessingOperationQueue.addOperation { [weak self] () -> Void in
            guard let weakSelf = self else { return }
            
            if let nextJob = weakSelf.nextJobToProcess()
            {
                weakSelf.currentJob = nextJob
                
                // Update delegate
                weakSelf.delegate?.worker(weakSelf, didStartProcessingJob: nextJob)
                
                // Execute job
                let result = weakSelf.worker.perform(nextJob.JSON)
                let shouldIncrementRetryCount = self?.dataSource.workerShouldIncrementRetryCounts() ?? true
                
                switch result
                {
                    case .success:
                        nextJob.delete()
                    case .fail where shouldIncrementRetryCount:
                        if nextJob.numberOfRetries >= weakSelf.maximumNumberOfRetries
                        {
                            nextJob.delete()
                        }
                        else
                        {
                            nextJob.incrementRetryCount()
                        }
                    default:()
                }
                
                // Update delegate
                weakSelf.delegate?.worker(weakSelf, didFinishProcessingJob: nextJob, withResult: result)
                
                // Process next job!
                weakSelf.isProcessing = false
                weakSelf.processNextJob()
            }
            else
            {
                weakSelf.isProcessing = false
            }
        }
    }
    
    // MARK: -
    
    /**
     If no jobs are added and therefore directory not touched, then jobs
     whose process data are in the future will not be processed until
     something new is added to the queue.
     */
    fileprivate func dispatchCheckJobsQueue()
    {
        let time = DispatchTime.now() + Double(Int64(20.0 * Double(NSEC_PER_SEC))) / Double(NSEC_PER_SEC)
        self.checkJobsTimerQueue.asyncAfter(deadline: time) { [weak self] in
            self?.processNextJob()
            self?.dispatchCheckJobsQueue()
        }
    }
    
    // MARK: -
    
    fileprivate func queueDirectoryUpdated()
    {
        self.processNextJob()
    }
    
    // MARK: Jobs
    
    fileprivate func nextJobToProcess() -> KyuJob?
    {
        let jobs = self.fetchAllJobs()
        return jobs.first
    }
    
    fileprivate func fetchAllJobs() -> [KyuJob]
    {
        let fileManager = FileManager.default
        guard let jobDirectoryNames = (try? fileManager.contentsOfDirectory(atPath: self.queueDirectoryPathURL.path)) else
        {
            return []
        }
        
        let jobs = jobDirectoryNames.flatMap({ (directoryName) -> URL? in
            return self.queueDirectoryPathURL.appendingPathComponent(directoryName)
        }).flatMap({ (directoryURL) -> KyuJob? in
            do
            {
                let job = try KyuJob(directoryURL: directoryURL)
                return job
            }
            catch
            {
                return nil
            }
        }).filter({ (job) -> Bool in
            return job.shouldProcess
        }).sorted(by: { (jobA, jobB) -> Bool in
            return jobA.processDate.compare(jobB.processDate as Date) == .orderedAscending
        })
        
        return jobs
    }
    
    internal func requestAllJobs(_ completionHandler: @escaping (_ jobs: [KyuJob]) -> Void)
    {
        self.jobFetchingOperationQueue.addOperation { [weak self] in
            guard let weakSelf = self else { return }
            
            let jobs = weakSelf.fetchAllJobs()
            completionHandler(jobs)
        }
    }
    
    // MARK: File system
    
    fileprivate func setupQueueDirectory() throws
    {
        try self.createDirectoryAtPath(self.queueDirectoryPathURL.path)
    }
    
    fileprivate func setupTemporaryDirectory() throws
    {
        try self.createDirectoryAtPath(self.temporaryDirectoryPathURL.path)
    }
    
    fileprivate func createDirectoryAtPath(_ directoryPath: String) throws
    {
        let fileManager = FileManager.default
        
        var isDirectory: ObjCBool = ObjCBool(false)
        if fileManager.fileExists(atPath: directoryPath, isDirectory: &isDirectory)
        {
            if !isDirectory.boolValue
            {
                // TODO: raise error?
            }
        }
        else
        {
            try fileManager.createDirectory(atPath: directoryPath, withIntermediateDirectories: true, attributes: nil)
        }
    }
}
